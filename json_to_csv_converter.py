import pandas as pd
import json

# Converting Product Catalog JSON to CSV to seed dbt models, dbt doesn't support JSON data seeding.
# Alternatively could use dbt-external-tables package to read JSON files directly from GCS/S3 external stage and batch merge.

with open("Data/product_catalog.json") as f:
    data = json.load(f)

df = pd.json_normalize(data)  # flatten nested fields

df.columns = [c.replace(".", "_") for c in df.columns] # replace dots in header with underscores

df.to_csv("dbt_service/seeds/product_catalog.csv", index=False)