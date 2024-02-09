import http.client
import urllib.parse

conn = http.client.HTTPSConnection("weatherapi-com.p.rapidapi.com")

headers = {
    'X-RapidAPI-Key': "2467bc2bd9mshfd02cfd8d4e5a77p13ba22jsn5eecabaf967e",
    'X-RapidAPI-Host': "weatherapi-com.p.rapidapi.com"
}

base_url = "/history.json"

# Constructing the parameters directly without urlencode
params = {
    'q': 'London',
    'dt': '2023-11-20',  # Example date after 2010-01-01
    'lang': 'en'
}

# Constructing the query string manually
query_string = '&'.join([f"{key}={urllib.parse.quote(str(value))}" for key, value in params.items()])
full_url = f"{base_url}?{query_string}"

conn.request("GET", full_url, headers=headers)

res = conn.getresponse()
data = res.read()

print(data.decode("utf-8"))
