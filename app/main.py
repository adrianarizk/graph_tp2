from fastapi import FastAPI, HTTPException
from neo4j import GraphDatabase
import os

app = FastAPI()

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/recs_item")
def recs_item(customer_id: str, k: int = 5):
    cy = """
    MATCH (c:Customer {id:$cid})-[:PLACED]->(:Order)-[:CONTAINS]->(p:Product)
    MATCH (p)-[r:CO_PURCHASE]-(s:Product)
    WHERE NOT EXISTS {
      MATCH (c)-[:PLACED]->(:Order)-[:CONTAINS]->(s)
    }
    RETURN s.id AS product_id, sum(r.weight) AS score
    ORDER BY score DESC
    LIMIT $k
    """
    with driver.session() as s:
        rows = list(s.run(cy, {"cid": customer_id, "k": k}))
    data = [{"product_id": r["product_id"], "score": r["score"]} for r in rows]
    if not data:
        raise HTTPException(status_code=404, detail="No recommendations found")
    return {"customer_id": customer_id, "items": data}


    with driver.session() as s:
        rows = list(s.run(cooc, {"cid": customer_id, "k": k}))
        data = [{"product_id": r["product_id"], "score": r["score"]} for r in rows]

        if not data:
            rows = list(s.run(pop_excluding_mine, {"cid": customer_id, "k": k}))
            data = [{"product_id": r["product_id"], "score": r["score"]} for r in rows]

        if not data:
            rows = list(s.run(pop_overall, {"k": k}))
            data = [{"product_id": r["product_id"], "score": r["score"]} for r in rows]

    if not data:
        raise HTTPException(status_code=404, detail="No recommendations found")
    return {"customer_id": customer_id, "items": data}

