import json
import os
from peopledatalabs import PDLPY
from dotenv import load_dotenv

# Create a client, specifying your API key
load_dotenv()
pdl_key = os.environ.get("PDL_API_KEY")
client = PDLPY(
    api_key=pdl_key,
)

# Create an SQL query
SQL_QUERY = "SELECT * FROM person WHERE (linkedin_url = 'linkedin.com/in/asafsavich')"

# Create a parameters JSON object
PARAMS = {
  'dataset': 'resume',
  'sql': SQL_QUERY,
  'size': 10,
  'pretty': True
}

# Pass the parameters object to the Person Search API
response = client.person.search(**PARAMS).json()

# Check for successful response
if response["status"] == 200:
  data = response['data']
  # Write out each profile found to file
  with open("my_pdl_search.jsonl", "w") as out:
    for record in data:
      out.write(json.dumps(record) + "")
  print(f"successfully grabbed {len(data)} records from pdl")
  print(f"{response['total']} total pdl records exist matching this query")
else:
  print("NOTE. The carrier pigeons lost motivation in flight. See error and try again.")
  print("Error:", response)