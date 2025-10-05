from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from llm import ask_ollama
from db import run_sql_query
from prompt_templates import SQL_PROMPT_TEMPLATE, RESPONSE_PROMPT_TEMPLATE

from sqlalchemy import create_engine
import json

# engine = create_engine("sqlite:///mobility.db")

app = FastAPI()

# Allow all origins for testing purposes (or specify your frontend origin URL)
origins = [
    "http://localhost",  # Nginx frontend (you can also add other URLs if needed)
    "http://localhost:80",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  # Allow requests from your frontend
    allow_credentials=True,
    allow_methods=["*"],  # Allow all HTTP methods (GET, POST, etc.)
    allow_headers=["*"],  # Allow all headers
)

TABLE_SCHEMA = """
Table: trips
- id (serial)
- mongo_id (varchar)
- VendorID (int)
- tpep_pickup_datetime (timestamp)
- tpep_dropoff_datetime (timestamp)
- passenger_count (int)
- trip_distance (float)
- RatecodeID (int)
- store_and_fwd_flag (varcahr)
- PULocationID (int)
- DOLocationID (int)
- payment_type (int)
- fare_amount (float)
- extra (float)
- mta_tax (float)
- tip_amount (float)
- tolls_amount (float)
- improvement_surcharge (float)
- congestion_surcharge (float)
- total_amount (float)
"""



# Favicon route (optional)
@app.get("/favicon.ico")
async def favicon():
    return FileResponse("favicon.ico")  # Replace with the correct favicon path if you have one


@app.post("/query")
async def query_nl(request: Request):
    data = await request.json()
    user_question = data["question"]

    # Step 1: Generate SQL from question
    sql_prompt = SQL_PROMPT_TEMPLATE.format(
        schema=TABLE_SCHEMA,
        question=user_question
    )
    sql_query = ask_ollama(sql_prompt).strip()

    if sql_query.startswith("```sql"):
        sql_query = sql_query.removeprefix("```sql").strip()
    if sql_query.endswith("```"):
        sql_query = sql_query.removesuffix("```").strip()

    print(f"[DEBUG] Generated SQL: {sql_query}")

    # Step 2: Run SQL
    try:
        results = run_sql_query(sql_query)
    except Exception as e:
        return {"error": str(e), "sql": sql_query}

    # Step 3: Generate natural language answer
    results_json = json.dumps(results, indent=2)
    response_prompt = RESPONSE_PROMPT_TEMPLATE.format(
        question=user_question,
        sql=sql_query,
        results=results_json
    )
    response_text = ask_ollama(response_prompt).strip()

    return {
        "sql": sql_query,
        "results": results,
        "response": response_text
    }
