# app/services/csv_service.py

import asyncio
import duckdb
import os
import pandas as pd


AVAILABLE_SYMBOLS = ["TATASTEEL", "GOOGL", "MSFT", "AMZN", "TSLA"]
LOCAL_DATA_PATH   = "data/stocks"


async def process_csv_from_s3(bucket, key, start_date, end_date):
    """
    Process CSV data — local first, S3 fallback.
    DuckDB queries run in thread pool to avoid blocking event loop.
    """
    try:
        ticker = key.split("/")[-1].replace(".csv", "").upper()

        # Check local first
        if ticker in AVAILABLE_SYMBOLS:
            print(f"Symbol {ticker} found in array, reading from local path...")

            local_file = os.path.join(LOCAL_DATA_PATH, f"{ticker}.csv")

            if os.path.exists(local_file):
                # Run pandas in thread pool — avoids blocking
                loop = asyncio.get_event_loop()
                df   = await loop.run_in_executor(
                    None, pd.read_csv, local_file
                )

                if "Date" in df.columns:
                    df["Date"] = pd.to_datetime(df["Date"])
                    mask = (
                        (df["Date"] >= start_date) &
                        (df["Date"] <= end_date)
                    )
                    filtered_df = df[mask].sort_values("Date")
                    filtered_df["Date"] = filtered_df["Date"].dt.strftime("%Y-%m-%d")

                    print(f"Returned data from local path for {ticker}")
                    return filtered_df.to_dict(orient="records")
                else:
                    raise Exception("Date column not found in CSV")
            else:
                print(f"Symbol {ticker} in array but file not found at {local_file}")
                AVAILABLE_SYMBOLS.remove(ticker)

        # S3 fallback — run DuckDB in thread pool
        print(f"Symbol {ticker} not in array, fetching from S3...")

        def _run_duckdb_query():
            con = duckdb.connect(database=":memory:")

            con.execute(f"""
                INSTALL httpfs;
                LOAD httpfs;
                SET s3_region='{os.getenv("AWS_REGION", "ap-south-1")}';
            """)

            aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
            aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

            if aws_access_key and aws_secret_key:
                con.execute(f"""
                    SET s3_access_key_id='{aws_access_key}';
                    SET s3_secret_access_key='{aws_secret_key}';
                """)

            query = f"""
                SELECT *
                FROM read_csv_auto('s3://{bucket}/{key}')
                WHERE CAST(Date AS DATE) BETWEEN '{start_date}' AND '{end_date}'
                ORDER BY Date
            """

            return con.execute(query).fetchdf()

        # Run blocking DuckDB in thread pool
        loop   = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, _run_duckdb_query)

        return result.to_dict(orient="records")

    except Exception as e:
        raise Exception(f"DuckDB S3 error: {str(e)}")