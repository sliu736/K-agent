# Databricks notebook source
# MAGIC %pip install openai

# COMMAND ----------

# MAGIC %pip install databricks_langchain

# COMMAND ----------

import ast
from databricks.sdk import WorkspaceClient
import os
from openai import OpenAI

# COMMAND ----------

userquestion = 'give me the recommended travel spots in San Francisco with accessibilitie. provide information with category, address, name and services_provided '

# COMMAND ----------

DATABRICKS_SPACE_ID = os.getenv("DATABRICKS_SPACE_ID")
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

import json
import logging
from typing import Dict, List, Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dashboards import GenieAPI

workspace_client = WorkspaceClient(
    host=DATABRICKS_HOST,
    token=DATABRICKS_TOKEN
)

genie_api = GenieAPI(workspace_client.api_client)
result = genie_api.create_message(
space_id='01f0457621c313b5ad95ab1984ec9cf6',
conversation_id='01f04576972b1f1b8be29d22219a6154'
,content=userquestion).result()

# COMMAND ----------

query_string = result.attachments[0].query.query if result and len(result.attachments) > 0 else None

# COMMAND ----------

df = spark.sql(query_string).toPandas()

# COMMAND ----------

# generating sample email drafts 
data = df.iloc[0]

# COMMAND ----------

def generate_accessibility_email(category, address, services_provided):
    services = ', '.join(services_provided)
    return f"""Subject: Inquiry Regarding Wheelchair Accessibility at Your {category}

Dear Sir or Madam,

I hope this message finds you well. I am planning to visit your {category.lower()} located at {address}, and I wanted to inquire about the availability and reliability of your wheelchair accessibility services.

According to available information, your facility provides the following services: {services}. I would greatly appreciate it if you could confirm that these features are currently available and functioning, and if there are any additional accessibility measures in place (e.g., elevator access, clearly marked accessible spots, assistance if needed).

Thank you very much for your support. I look forward to your response.

"""

email = generate_accessibility_email(
    category=data['category'],
    address=data['address'],
    services_provided=ast.literal_eval(data['services_provided'])
)
print(email)



# COMMAND ----------

usercontent = """
I want recommnedations to plan my day visit including 3 parks in SFO and have a good breakfast, lunch and Itlian dinner. I need wheelchair assistance. Here is the data I have {df}
"""

# COMMAND ----------


w = WorkspaceClient()

os.environ["DATABRICKS_HOST"] = w.config.host
os.environ["DATABRICKS_TOKEN"] = w.tokens.create(comment="for model serving", lifetime_seconds=1200).token_value


tmp_token = w.tokens.create(lifetime_seconds=2400).token_value
client = OpenAI(
    api_key=tmp_token,
    base_url=f"{w.config.host}/serving-endpoints",
)
chat_completion = client.chat.completions.create(
    messages=[
        {"role": "system", "content": 
            """
            You are a specialized tour planner for people with disabilities.
            """
},
        {"role": "user", "content": usercontent},
    ],
    model="databricks-llama-4-maverick",
    max_tokens=1000,
)

# COMMAND ----------

print(chat_completion.choices[0].message.content)

# COMMAND ----------

#Configure a email agent to send the visit plan to the visitors and specific notifications to the location supervisors for coordination.

# COMMAND ----------

# Configure a uber/waymo bot to plan the travel from location A to location B