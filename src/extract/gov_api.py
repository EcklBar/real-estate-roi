import requests

# The API endpoint
url = "https://data.gov.il/api/3/action/help_show?name=datastore_search"

# A GET request to the API
response = requests.get(url)

# Print the response
print(response.json())
