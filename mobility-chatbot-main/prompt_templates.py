# SQL query generation
SQL_PROMPT_TEMPLATE = """
You are an assistant that translates natural language into PostgreSQL queries.

Given the user's question and the following table schema:

{schema}

Translate the following question into an PostgreSQL query:

"{question}"

Only return the PostgreSQL query, without explanations or formatting.

The columns tpep_pickup_datetime and tpep_dropoff_datetime are TIMESTAMP (date + time).
Only use DATE(), EXTRACT(), or DATE_PART() when the question explicitly mentions a specific date, day, month, or year.
If the question does not mention time or date filters, do NOT filter by date or time.

Examples:
-- specific date:
WHERE DATE(tpep_pickup_datetime) = '2020-01-01';

-- specific month/year:
WHERE EXTRACT(MONTH FROM tpep_pickup_datetime) = 1 AND EXTRACT(YEAR FROM tpep_pickup_datetime) = 2020;

-- no date mentioned:
SELECT COUNT(*) FROM trips;
"""


# Natural language response generation
RESPONSE_PROMPT_TEMPLATE = """
You are an assistant for a mobility company. A user asked: "{question}"

Here is the PostgreSQL that was generated:
{sql}

And here are the results (as a JSON list of records):
{results}

Write a clear, concise, and helpful answer that summarizes the results for an executive.
"""
