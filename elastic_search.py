from elasticsearch import Elasticsearch, helpers
import json

# Connect to Elasticsearch
es = Elasticsearch(
    "https://localhost:9200",
    basic_auth=("elastic", "password"),
    ca_certs=r"D:\elasticsearch-9.1.4-windows-x86_64\elasticsearch-9.1.4\config\certs\http_ca.crt"
)

# Sample dataset
users = [
  {"id": 1, "name": "Ajay Kumar", "age": 30, "city": "Chennai"},
  {"id": 2, "name": "Vijay Sharma", "age": 25, "city": "Mumbai"},
  {"id": 3, "name": "Priya Reddy", "age": 35, "city": "Chennai"},
  {"id": 4, "name": "Madhu Nair", "age": 28, "city": "Bengaluru"},
  {"id": 5, "name": "Ravi Singh", "age": 32, "city": "Delhi"},
  {"id": 6, "name": "Anita Patel", "age": 27, "city": "Hyderabad"},
  {"id": 7, "name": "Kiran Das", "age": 29, "city": "Pune"},
  {"id": 8, "name": "Deepak Choudhury", "age": 40, "city": "Kolkata"},
  {"id": 9, "name": "Sneha Iyer", "age": 24, "city": "Chennai"},
  {"id": 10, "name": "Rahul Verma", "age": 33, "city": "Mumbai"},
  {"id": 11, "name": "Meena Joshi", "age": 31, "city": "Delhi"},
  {"id": 12, "name": "Suresh Gowda", "age": 26, "city": "Hyderabad"},
  {"id": 13, "name": "Lakshmi Menon", "age": 29, "city": "Bengaluru"},
  {"id": 14, "name": "Arjun Kapoor", "age": 34, "city": "Pune"},
  {"id": 15, "name": "Divya Rao", "age": 27, "city": "Kolkata"}
]

# Index a document
# es.index(index="users", id=1, document={"name": "Ajay Kumar", "age": 30, "city": "Chennai"})

# Bulk insert
actions = [{"_index": "users", "_id": u["id"], "_source": u} for u in users]
helpers.bulk(es, actions)

# Get a document
print(json.dumps(es.get(index="users", id=1), indent=2))

# Update a document (set age = 31)
es.update(index="users", id=1, body={"doc": {"age": 31}})
print(json.dumps(es.get(index="users", id=1), indent=2))

# Delete a document
es.delete(index="users", id=1)

# Match query (search for 'Ajay' in name)
print(json.dumps(es.search(index="users", query={"match": {"name": "Ajay"}}), indent=2))

# Term query (exact filter by city = Chennai)
print(json.dumps(es.search(index="users", query={"term": {"city.keyword": "Chennai"}}), indent=2))

# Range query (age between 25–30)
print(json.dumps(es.search(index="users", query={"range": {"age": {"gte": 25, "lte": 30}}}), indent=2))

# Bool query (city = Bengaluru AND age >= 25)
print(json.dumps(es.search(
    index="users",
    query={"bool": {"must": [
        {"term": {"city.keyword": "Bengaluru"}},
        {"range": {"age": {"gte": 25}}}
    ]}}
), indent=2))

# Aggregation: average age of all users
print(json.dumps(es.search(index="users", size=0, aggs={"avg_age": {"avg": {"field": "age"}}}), indent=2))

# Aggregation: group by city
print(json.dumps(es.search(index="users", size=0, aggs={"users_per_city": {"terms": {"field": "city.keyword"}}}), indent=2))

# Aggregation: average age per city
print(json.dumps(es.search(
    index="users",
    size=0,
    aggs={"cities": {"terms": {"field": "city.keyword"}, "aggs": {"avg_age": {"avg": {"field": "age"}}}}}
), indent=2))
