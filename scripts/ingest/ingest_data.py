# ...existing code...
import argparse
import os
import re
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


def normalize_column_names(df):
    """
    Convert column names to safe lowercase snake_case:
    - strip
    - replace anything not alnum with underscore
    - collapse multiple underscores
    - trim leading/trailing underscores
    """
    cols = []
    for c in df.columns:
        s = str(c).strip().lower()
        # replace any non-alphanumeric with underscore
        s = re.sub(r'[^a-z0-9]+', '_', s)
        s = re.sub(r'_+', '_', s).strip('_')
        if not s:
            s = 'col'
        cols.append(s)
    df.columns = cols
    return df

def validate_identifier(name, what):
    if not re.match(r'^[A-Za-z_][A-Za-z0-9_]*$', name):
        raise ValueError(f"Invalid {what} name: {name!r}. Use only letters, numbers and underscores, not starting with a digit.")

def main(file_path, schema, table):
    # Get DB URL from environment
    db_url = os.getenv("DB_URL")
    if not db_url:
        raise ValueError("DB_URL not found in environment variables (.env file)")

    # validate inputs
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Input file not found: {file_path}")
    validate_identifier(schema, "schema")
    validate_identifier(table, "table")

    # Create SQLAlchemy engine
    engine = create_engine(db_url)

    # Read dataset (auto-detect type)
    print(f"Reading file: {file_path}")
    if file_path.endswith(".parquet"):
        df = pd.read_parquet(file_path)
    elif file_path.endswith(".csv"):
        df = pd.read_csv(file_path)
    else:
        raise ValueError("Unsupported file format. Use .parquet or .csv")

    # Normalize column names
    df = normalize_column_names(df)

    # Ensure schema exists (use a transaction)
    with engine.begin() as conn:
        conn.exec_driver_sql(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')

    # Write to database
    print(f"Ingesting into {schema}.{table} ...")
    df.to_sql(name=table, con=engine, schema=schema, if_exists="replace", index=False)

    print(f"Ingested {len(df)} rows into {schema}.{table}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest data into Supabase/Postgres")
    # make file an option/flag instead of positional so --file works
    parser.add_argument("-f", "--file", dest="file", required=True,
                        help="Path to input file (.parquet or .csv)")
    parser.add_argument("--schema", default="raw", help="Target schema (default: raw)")
    parser.add_argument("--table", default="transactions_raw", help="Target table name (default: transactions_raw)")

    args = parser.parse_args()

    main(args.file, args.schema, args.table)