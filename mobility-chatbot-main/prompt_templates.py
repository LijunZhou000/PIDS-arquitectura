# SQL query generation
SQL_PROMPT_TEMPLATE = """
You are an assistant that translates natural language into SQL queries.

Given the user's question and the following table schema:

{schema}

Translate the following question into an SQL query:

"{question}"
Take into account that the total amount spent by the clients is provided by total_amount, while the amount of money the company gains is obtained by adding fare_amount and extra.
Only return the SQL query, without explanations or formatting.
"""


# Natural language response generation
RESPONSE_PROMPT_TEMPLATE = """
You are an assistant for a mobility company. A user asked: "{question}"

And here is the SQL that was generated:
{sql}

And here are the results (as a JSON list of records):
{results}

Write a clear, concise, and helpful answer that summarizes the results obtained for an executive.
"""
