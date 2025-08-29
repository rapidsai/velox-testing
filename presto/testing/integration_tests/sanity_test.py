import os
import prestodb

def test_simple_query():
    # Use environment variables with sensible defaults
    host = os.getenv("PRESTO_HOST", "localhost")
    port = int(os.getenv("PRESTO_PORT", "8080"))
    user = os.getenv("PRESTO_USER", "test_user")
    catalog = os.getenv("PRESTO_CATALOG", "tpch")
    schema = os.getenv("PRESTO_SCHEMA", "sf1")
    
    conn = prestodb.dbapi.connect(
        host=host, 
        port=port, 
        user=user, 
        catalog=catalog, 
        schema=schema
    )
    cursor = conn.cursor()

    cursor.execute("select count(*) from customer")
    rows = cursor.fetchall()

    assert len(rows) == 1
    assert len(rows[0]) == 1
    assert rows[0][0] == 150000
