from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from llm import ask_ollama
from db import run_sql_query
from prompt_templates import SQL_PROMPT_TEMPLATE, RESPONSE_PROMPT_TEMPLATE

from sqlalchemy import create_engine
import json
from decimal import Decimal

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
- id (serial, unique trip id)
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
- fare_amount (float, base fare estimated by taximeter)
- extra (float, additional standard fees like night surcharge and rush hour surcharge)
- mta_tax (float, fixed tax charged by the Metropolitan Transportation Authority)
- tip_amount (float, tip given to the driver)
- tolls_amount (float, total cost of tolls during the trip)
- improvement_surcharge (float, fixed fee added to yellow taxi trips)
- congestion_surcharge (float, fee introduced to reduce traffic by yellow taxis)
- total_amount (float, total amount paid by the passenger)
"""

def convert_decimals(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, list):
        return [convert_decimals(i) for i in obj]
    elif isinstance(obj, dict):
        return {k: convert_decimals(v) for k, v in obj.items()}
    else:
        return obj

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
    results = convert_decimals(results)
    results_json = json.dumps(results, indent=2, default=str)
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
