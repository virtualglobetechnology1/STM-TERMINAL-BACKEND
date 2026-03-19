import duckdb
import os
import pandas as pd
from app.utils.response import success_response, error_response

# Simple array of available symbols (you can populate this)
AVAILABLE_SYMBOLS = ["TATASTEEL", "GOOGL", "MSFT", "AMZN", "TSLA"]  # Add more symbols here

# Local path for CSV files
LOCAL_DATA_PATH = "data/stocks"  # Change this to your local path

def process_csv_from_s3(bucket, key, start_date, end_date):
    try:
        # Extract ticker from key (e.g., "stocks-data-2013-2025/AAPL.csv" -> "AAPL")
        ticker = key.split('/')[-1].replace('.csv', '').upper()
        
        # ✅ Check if symbol is in the array
        if ticker in AVAILABLE_SYMBOLS:
            print(f"📂 Symbol {ticker} found in array, reading from local path...")
            
            # Read from local path
            local_file = os.path.join(LOCAL_DATA_PATH, f"{ticker}.csv")
            
            if os.path.exists(local_file):
                # Read CSV with pandas
                df = pd.read_csv(local_file)
                
                # Filter by date range
                if 'Date' in df.columns:
                    df['Date'] = pd.to_datetime(df['Date'])
                    mask = (df['Date'] >= start_date) & (df['Date'] <= end_date)
                    filtered_df = df[mask].sort_values('Date')
                    
                    # Convert dates back to string
                    filtered_df['Date'] = filtered_df['Date'].dt.strftime('%Y-%m-%d')
                    
                    print(f"✅ Returned data from local path for {ticker}")
                    return filtered_df.to_dict(orient="records")
                else:
                    return error_response("Date column not found in CSV")
            else:
                print(f"⚠️ Symbol {ticker} in array but file not found at {local_file}")
                # Remove from array if file doesn't exist
                AVAILABLE_SYMBOLS.remove(ticker)
        
        # ✅ If not in array or file not found, fetch from S3
        print(f"🌐 Symbol {ticker} not in array, fetching from S3...")
        
        # Create connection
        con = duckdb.connect(database=':memory:')

        # Configure S3 access
        con.execute(f"""
            INSTALL httpfs;
            LOAD httpfs;

            SET s3_region='{os.getenv("AWS_REGION", "ap-south-1")}';
        """)

        # Optional (if not using IAM role)
        aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

        if aws_access_key and aws_secret_key:
            con.execute(f"""
                SET s3_access_key_id='{aws_access_key}';
                SET s3_secret_access_key='{aws_secret_key}';
            """)

        # Direct query from S3
        query = f"""
            SELECT *
            FROM read_csv_auto('s3://{bucket}/{key}')
            WHERE CAST(Date AS DATE) BETWEEN '{start_date}' AND '{end_date}'
            ORDER BY Date
        """

        result = con.execute(query).fetchdf()
        
        # Optional: Add to array for future requests
        # AVAILABLE_SYMBOLS.append(ticker)  # Uncomment if you want to add to array
        
        return result.to_dict(orient="records")

    except Exception as e:
        raise Exception(f"DuckDB S3 error: {str(e)}")