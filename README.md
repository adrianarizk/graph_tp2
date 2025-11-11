Author: Adriana Rizk
Course: Graph Databases 
ESSEC–CentraleSupélec Bachelor in AI, Data & Management Sciences



#  TP2 – Graph Databases (PostgreSQL → Neo4j ETL)

This project demonstrates how to extract relational data from **PostgreSQL**, transform it, and load it into **Neo4j** to visualize relationships in a graph structure.  
It was done as part of the **Graph Database module (S5)**.

---

##  1. Setup

###  PostgreSQL container
```bash
docker exec -it tp2_postgres psql -U app -d shop
Check the tables:
\dt
SELECT * FROM customers LIMIT 5;

Neo4j container
Access the Neo4j Browser and verify the databases:


SHOW DATABASES;

API Health Check
Verify that the FastAPI backend is running:


GET /health

2. ETL Process
Run the ETL pipeline that extracts data from PostgreSQL and loads it into Neo4j:


docker exec -it tp2_app bash
python etl.py
If everything works, you should see:


ETL done.

3. Graph Exploration
After running the ETL, you can visualize the data in Neo4j Browser.

Overview

MATCH (n) RETURN n LIMIT 25;

Customers

MATCH (c:Customer) RETURN c LIMIT 5;

Orders placed by customers
MATCH (c:Customer)-[:PLACED]->(o:Order) RETURN c,o LIMIT 5;

4. Product Relationships
Co-Purchases
MATCH (p1:Product)-[r:CO_PURCHASE]-(p2:Product)
RETURN p1.name, p2.name, r.weight LIMIT 10;


5. Queries and Analysis
Alice’s Purchases
MATCH (c:Customer {name:"Alice"})-[:PLACED]->(:Order)-[:CONTAINS]->(p:Product)
RETURN p;

Number of Products per Category


MATCH (p:Product)-[:IN_CATEGORY]->(cat:Category)
RETURN cat.name AS Category, COUNT(p) AS NbProducts
ORDER BY NbProducts DESC;

Most Viewed Products

MATCH (:Customer)-[r:VIEWED]->(p:Product)
RETURN p.name AS Product, COUNT(r) AS Views
ORDER BY Views DESC LIMIT 5;

Products Added to Cart
MATCH (:Customer)-[r:ADD_TO_CARTED]->(p:Product)
RETURN p.name AS Product, COUNT(r) AS AddedToCart
ORDER BY AddedToCart DESC;

Customer-to-Product Path (Example: Chloé)
MATCH path=(c:Customer {name:"Chloé"})-[*1..3]->(p:Product)
RETURN path;

Summary
This project connects:

A PostgreSQL relational schema (customers, orders, products)

A FastAPI ETL backend

A Neo4j graph database

to demonstrate how to transform e-commerce data into a graph structure that reveals relationships between customers, orders, and products.

