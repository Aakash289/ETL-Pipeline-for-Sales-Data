from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import pandas as pd
from sqlalchemy import create_engine

# PostgreSQL connection string
POSTGRES_CONN_STR = "postgresql+psycopg2://<username>:<password>@<host>:<port>/<database>"

default_args = {
    'owner': 'data_engineer',
    'start_date': days_ago(1),
}

@dag(schedule_interval='@daily', default_args=default_args, catchup=False, tags=["ETL", "Sales"])
def sales_etl_pipeline():

    @task()
    def extract_data():
        file_path = "/usr/local/airflow/include/data.csv"
        df = pd.read_csv(file_path, encoding="ISO-8859-1")
        return df.to_dict(orient='records')
    
    @task()
    def transform_convert_invoice_date(records: list):
        df = pd.DataFrame(records)
        df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'], errors='coerce')
        df['InvoiceDate'] = df['InvoiceDate'].astype(str) 
        return df.to_dict(orient='records')

    @task()
    def transform_calculate_revenue(records: list):
        df = pd.DataFrame(records)
        df['Revenue'] = df['Quantity'] * df['UnitPrice']
        return df.to_dict(orient='records')

    @task()
    def transform_filter_data(records: list):
        df = pd.DataFrame(records)
        df = df[df['Quantity'] > 0]
        df.dropna(subset=['CustomerID'], inplace=True)
        return df.to_dict(orient='records')

    @task()
    def load_to_postgres(records: list):
        df = pd.DataFrame(records)
        engine = create_engine(POSTGRES_CONN_STR)
        df.to_sql('cleaned_sales', engine, if_exists='replace', index=False)
        print("✅ Cleaned data loaded to PostgreSQL")

    raw_data = extract_data()
    with_dates = transform_convert_invoice_date(raw_data)
    with_revenue = transform_calculate_revenue(with_dates)
    final_data = transform_filter_data(with_revenue)
    load_to_postgres(final_data)

sales_etl_pipeline()