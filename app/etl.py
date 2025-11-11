import os, time
import pandas as pd
import psycopg2
from neo4j import GraphDatabase
from pathlib import Path

DB = dict(
    host=os.getenv("DB_HOST","postgres"),
    port=int(os.getenv("DB_PORT","5432")),
    db=os.getenv("DB_NAME","shop"),
    user=os.getenv("DB_USER","app"),
    pwd=os.getenv("DB_PASSWORD","app"),
)

NEO4J = dict(
    uri=os.getenv("NEO4J_URI","bolt://neo4j:7687"),
    user=os.getenv("NEO4J_USER","neo4j"),
    pwd=os.getenv("NEO4J_PASSWORD","password"),
)

def wait(pg=True, n4j=True):
    if pg:
        for _ in range(60):
            try:
                psycopg2.connect(host=DB["host"], port=DB["port"], dbname=DB["db"], user=DB["user"], password=DB["pwd"]).close()
                break
            except Exception:
                time.sleep(2)
    if n4j:
        for _ in range(60):
            try:
                GraphDatabase.driver(NEO4J["uri"], auth=(NEO4J["user"], NEO4J["pwd"])).verify_connectivity()
                break
            except Exception:
                time.sleep(2)

def run_cypher(cy, params=None):
    with GraphDatabase.driver(NEO4J["uri"], auth=(NEO4J["user"], NEO4J["pwd"])) as d, d.session() as s:
        s.run(cy, params or {})

def run_cypher_file(path: Path):
    text = path.read_text(encoding="utf-8")
    for stmt in [t.strip() for t in text.split(";") if t.strip()]:
        run_cypher(stmt)

def df(sql):
    with psycopg2.connect(host=DB["host"], port=DB["port"], dbname=DB["db"], user=DB["user"], password=DB["pwd"]) as conn:
        return pd.read_sql(sql, conn)

def etl():
    wait()

    # constraints
    run_cypher_file(Path(__file__).with_name("queries.cypher"))

    customers = df("SELECT id, name, join_date FROM customers")
    categories = df("SELECT id, name FROM categories")
    products  = df("SELECT id, name, price, category_id FROM products")
    orders    = df("SELECT id, customer_id, ts FROM orders")
    items     = df("SELECT order_id, product_id, quantity FROM order_items")
    events    = df("SELECT id, customer_id, product_id, event_type, ts FROM events")

    cy_nodes = "UNWIND $rows AS r MERGE (n:%(label)s {id:r.id}) SET n += r.props"
    def load_nodes(df_, label, props):
        rows = [{"id": r["id"], "props": {k: r[k] for k in props}} for _, r in df_.iterrows()]
        run_cypher(cy_nodes % {"label": label}, {"rows": rows})

    load_nodes(customers, "Customer", ["name","join_date"])
    load_nodes(categories, "Category", ["name"])
    load_nodes(products,  "Product",  ["name","price","category_id"])
    load_nodes(orders,    "Order",    ["ts","customer_id"])

    cy_rel = "UNWIND $rows AS r MATCH (a:%(la)s {id:r.a}),(b:%(lb)s {id:r.b}) MERGE (a)-[rel:%(type)s]->(b) SET rel += r.props"
    run_cypher(cy_rel % {"la":"Customer","lb":"Order","type":"PLACED"},
               {"rows":[{"a": r["customer_id"], "b": r["id"], "props": {}} for _, r in orders.iterrows()]})
    run_cypher(cy_rel % {"la":"Order","lb":"Product","type":"CONTAINS"},
               {"rows":[{"a": r["order_id"], "b": r["product_id"], "props": {"quantity": int(r["quantity"])}} for _, r in items.iterrows()]})
    run_cypher(cy_rel % {"la":"Product","lb":"Category","type":"IN_CATEGORY"},
               {"rows":[{"a": r["id"], "b": r["category_id"], "props": {}} for _, r in products.iterrows()]})
    # -- Construire les liens de co-achat en recalculant le poids à chaque ETL --
    co_purchase = """
    MATCH (o:Order)-[:CONTAINS]->(p1:Product),
      (o)-[:CONTAINS]->(p2:Product)
      WHERE id(p1) < id(p2)
      WITH p1, p2, count(*) AS w
      MERGE (p1)-[r:CO_PURCHASE]-(p2)
      SET r.weight = w
      """
    run_cypher(co_purchase)


    cy_event = """
    UNWIND $rows AS r
    MATCH (c:Customer {id:r.c}),(p:Product {id:r.p})
    CALL apoc.merge.relationship(c, r.t, {}, r.props, p) YIELD rel
    RETURN count(rel)
    """
    rows = [{"c": r["customer_id"], "p": r["product_id"], "t": r["event_type"].upper()+"ED", "props": {"ts": r["ts"]}}
            for _, r in events.iterrows()]
    run_cypher(cy_event, {"rows": rows})
    print("ETL done.")

if __name__ == "__main__":
    etl()

