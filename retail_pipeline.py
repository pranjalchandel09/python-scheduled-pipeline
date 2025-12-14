import os
import logging
import pandas as pd
import snowflake.connector

logging.basicConfig(
    filename="pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def main():
    logging.info("Pipeline started")

    # Connect to Snowflake using GitHub Secrets
    conn = snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"]
    )

    logging.info("Connected to Snowflake")

    # Load data
    query = "SELECT * FROM TRANSACTION_RAW LIMIT 100"
    df = pd.read_sql(query, conn)

    logging.info(f"Rows loaded: {len(df)}")

    # Save data locally (proof that data was loaded)
    df.to_csv("processed_data.csv", index=False)
    logging.info("Data saved to processed_data.csv")

    conn.close()
    logging.info("Pipeline finished successfully")

if __name__ == "__main__":
    main()


