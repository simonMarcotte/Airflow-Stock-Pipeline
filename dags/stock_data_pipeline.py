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
from alpha_vantage.alphaintelligence import AlphaIntelligence
from typing import Tuple


@dag(
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    tags=["stock_data"],
    catchup=False
)

def stock_data_pipeline():

    @task
    def get_stock_data() -> str:

        API_KEY = Variable.get("alt_api_key")
        filepath = Variable.get("stock_data")
        ts = TimeSeries(key=API_KEY)

        alphai = AlphaIntelligence(key=API_KEY)
        ret = alphai.get_top_gainers()
        top_gainer = ret[0].iloc[0]['ticker']

        data, meta_data = ts.get_daily(top_gainer, outputsize='full')

        df = pd.DataFrame.from_dict(data, orient='index')
        df.reset_index(inplace=True)
        df.columns = ['date', 'open', 'high', 'low', 'close', 'volume']
        df['ticker'] = top_gainer
        df.to_csv(filepath, index=False)

        return filepath
    
    @task
    def upload_to_bigquery(filepath: str) -> Tuple[str, str]:

        TABLE_ID = Variable.get("historical_stock_table_id")
        client = bigquery.Client()

        table_id = TABLE_ID
        df = pd.read_csv(filepath)
        job = client.load_table_from_dataframe(df, table_id)
        job.result()

        return {'table': table_id, 'ticker': df['ticker'].iloc[0]}
    
    @task
    def calculate_max_profit(stock_info: dict) -> Tuple[float, str]:

        client = bigquery.Client()
        top_gainer = stock_info['ticker']
        query = f"""
            SELECT * FROM `{stock_info['table']}`
            WHERE ticker = '{top_gainer}'
            ORDER BY date
        """

        df2 = client.query(query).to_dataframe()

        max_profit = 0
        min_price = float('inf')

        for index, row in df2.iterrows():
            price = float(row['low'])
            min_price = min(min_price, price)
            profit = float(row['high']) - min_price
            max_profit = max(profit, max_profit)

        print(f"The maximum profit for {top_gainer} is: {max_profit}")

        return {'profit': max_profit, 'ticker': top_gainer, 'date': df2['date'].iloc[-1]}
    
    @task
    def store_max_profit(profit_info: dict) -> None:


        client = bigquery.Client()
        MAX_PROFITS_TABLE_ID = Variable.get("max_profits_table_id")
        rows_to_insert = [
            {"ticker": profit_info['ticker'], "date": profit_info['date'], "max_profit": profit_info['profit']}
        ]
        errors = client.insert_rows_json(MAX_PROFITS_TABLE_ID, rows_to_insert)
        if errors:
            print(f"Encountered errors while inserting rows: {errors}")
        else:
            print(f"Max profit for {profit_info['ticker']} successfully stored.")

    filepath = get_stock_data()
    stock_info = upload_to_bigquery(filepath)
    profit_info = calculate_max_profit(stock_info)
    store_max_profit(profit_info)

stock_data_pipeline()
