from elasticsearch import Elasticsearch, helpers
import json

# Connect to Elasticsearch
es = Elasticsearch(
    "https://localhost:9200",
    basic_auth=("elastic", "password"),
    ca_certs=r"D:\elasticsearch-9.1.4-windows-x86_64\elasticsearch-9.1.4\config\certs\http_ca.crt"
)

# Sample dataset
products = [
  {"id": 1, "name": "Samsung Galaxy S24 Ultra", "category": "Smartphone", "price": 1299, "stock": 50},
  {"id": 2, "name": "Samsung Galaxy S24+", "category": "Smartphone", "price": 1099, "stock": 70},
  {"id": 3, "name": "Samsung Galaxy S24", "category": "Smartphone", "price": 899, "stock": 100},
  {"id": 4, "name": "Samsung Galaxy Z Fold5", "category": "Smartphone", "price": 1799, "stock": 30},
  {"id": 5, "name": "Samsung Galaxy Z Flip5", "category": "Smartphone", "price": 999, "stock": 40},
  {"id": 6, "name": "Samsung Galaxy Tab S9 Ultra", "category": "Tablet", "price": 1199, "stock": 25},
  {"id": 7, "name": "Samsung Galaxy Tab S9+", "category": "Tablet", "price": 999, "stock": 35},
  {"id": 8, "name": "Samsung Galaxy Tab S9", "category": "Tablet", "price": 799, "stock": 50},
  {"id": 9, "name": "Samsung Galaxy Book3 Ultra", "category": "Laptop", "price": 2399, "stock": 15},
  {"id": 10, "name": "Samsung Galaxy Book3 Pro", "category": "Laptop", "price": 1799, "stock": 20},
  {"id": 11, "name": "Samsung QN90C Neo QLED 4K TV", "category": "TV", "price": 1999, "stock": 10},
  {"id": 12, "name": "Samsung QN85C Neo QLED 4K TV", "category": "TV", "price": 1499, "stock": 12},
  {"id": 13, "name": "Samsung The Frame 55\" 4K TV", "category": "TV", "price": 1299, "stock": 18},
  {"id": 14, "name": "Samsung Galaxy Buds2 Pro", "category": "Accessories", "price": 229, "stock": 80},
  {"id": 15, "name": "Samsung Galaxy Watch6 Classic", "category": "Accessories", "price": 399, "stock": 60}
]

# Index a document
# es.index(index="users", id=1, document={"name": "Samsung Galaxy S24 Ultra", "category": "Smartphone", "price": 1299, "stock": 50})

# Bulk insert for products
actions = [{"_index": "products", "_id": p["id"], "_source": p} for p in products]
helpers.bulk(es, actions)

# Get a document
print(json.dumps(es.get(index="products", id=1), indent=2))

# Update a document (set stock = 45 for id=1)
es.update(index="products", id=1, body={"doc": {"stock": 45}})
print(json.dumps(es.get(index="products", id=1), indent=2))

# Delete a document
es.delete(index="products", id=1)

# Match query (search for 'Galaxy' in product name)
print(json.dumps(es.search(index="products", query={"match": {"name": "Galaxy"}}), indent=2))

# Term query (exact filter by category = Smartphone)
print(json.dumps(es.search(index="products", query={"term": {"category.keyword": "Smartphone"}}), indent=2))

# Range query (price between 500–1500)
print(json.dumps(es.search(index="products", query={"range": {"price": {"gte": 500, "lte": 1500}}}), indent=2))

# Bool query (category = TV AND price >= 1500)
print(json.dumps(es.search(
    index="products",
    query={"bool": {"must": [
        {"term": {"category.keyword": "TV"}},
        {"range": {"price": {"gte": 1500}}}
    ]}}
), indent=2))

# Aggregation: average price of all products
print(json.dumps(es.search(index="products", size=0, aggs={"avg_price": {"avg": {"field": "price"}}}), indent=2))

# Aggregation: group by category
print(json.dumps(es.search(index="products", size=0, aggs={"products_per_category": {"terms": {"field": "category.keyword"}}}), indent=2))

# Aggregation: average price per category
print(json.dumps(es.search(
    index="products",
    size=0,
    aggs={"categories": {"terms": {"field": "category.keyword"}, "aggs": {"avg_price": {"avg": {"field": "price"}}}}}
), indent=2))
