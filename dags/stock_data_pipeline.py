"""
DAG Description: a job to run daily to parse the historical stock data for GOOGL over the last 20 years, 
and find the maximum profit the stock has gained
"""

from airflow.decorators import dag, task
from airflow.models import Variable
from pendulum import datetime
import pandas as pd
from google.cloud import bigquery
from alpha_vantage.timeseries import TimeSeries


@dag(
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    tags=["stock_data"],
    catchup=False
)

def stock_data_pipeline():

    @task
    def get_stock_data():

        API_KEY = Variable.get("secret_alpha_vantage_api_key")
        filepath = Variable.get("stock_data")
        ts = TimeSeries(key=API_KEY)

        data, meta_data = ts.get_daily('GOOGL', outputsize='full')

        df = pd.DataFrame.from_dict(data, orient='index')
        df.reset_index(inplace=True)
        df.columns = ['date', 'open', 'high', 'low', 'close', 'volume']
        df['timestamp'] = pd.to_datetime(df['date'])
        df = df[['timestamp', 'high', 'low']]
        df.to_csv(filepath, index=False)

        return filepath
    
    @task
    def upload_to_bigquery(filepath):

        TABLE_ID = Variable.get("historical_stock_table_id")
        client = bigquery.Client()

        table_id = TABLE_ID
        df = pd.read_csv(filepath)
        job = client.load_table_from_dataframe(df, table_id)
        job.result()

        return table_id
    
    @task
    def calculate_max_profit(table_id):

        client = bigquery.Client()
        query = f"""
            SELECT * FROM `{table_id}`
            ORDER BY timestamp
        """

        df2 = client.query(query).to_dataframe()

        max_profit = 0
        min_price = float('inf')

        for index, row in df2.iterrows():
            price = float(row['low'])
            min_price = min(min_price, price)
            profit = float(row['high']) - min_price
            max_profit = max(profit, max_profit)

        print(f"The maximum profit is: {max_profit}")

        return max_profit

    stock_data_file = get_stock_data()
    bigquery_table = upload_to_bigquery(stock_data_file)
    calculate_max_profit(bigquery_table)

stock_data_pipeline()
