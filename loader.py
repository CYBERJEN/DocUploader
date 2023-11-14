import requests
import boto3
import sqlite3
import config
connection = sqlite3.connect(config.DB_PATH)
links = connection.execute("select id, Link,PropertyProcessNo, DocumentType from Documents").fetchAll()
client = boto3.client("s3")
for uid, link, process,documenttype in links:
    response = requests.get(link)
    client.put_object(
        Body= response.content, 
        Bucket= "condorepository",
        Key = f"{process}-{documenttype}-{uid}.pdf"

    )
    print("Document is Loaded", process)
