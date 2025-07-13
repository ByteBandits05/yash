# smoketest/smoke_test.py
# Purpose: Smoke Test Script - Validates presence and content of a specified Databricks table using OIDC credentials

import os
import sys
from databricks import sql

table_name = os.getenv("SMOKE_TEST_TABLE")
server_hostname = os.getenv("DATABRICKS_HOST_DEV")
http_path = os.getenv("DATABRICKS_SQL_HTTP_PATH")
access_token = os.getenv("DATABRICKS_TOKEN")

if not all([table_name, server_hostname, http_path, access_token]):
    print("Missing environment variables for Databricks connection or table name.")
    sys.exit(1)

try:
    with sql.connect(
        server_hostname=server_hostname,
        http_path=http_path,
        access_token=access_token
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            if count < 1:
                print(f"Table {table_name} exists but is empty.")
                sys.exit(1)
            print(f"Smoke test passed: Table {table_name} has {count} rows.")
except Exception as e:
    print(f"Smoke test failed: {e}")
    sys.exit(1)
