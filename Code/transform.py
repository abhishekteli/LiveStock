from pyspark.sql.functions import *


class TransformData:

    def populateFields(self, stockData_df):
        stockData_df = stockData_df.fillna({'pre_or_post_market': 0, 'pre_or_post_market_change': 0})
        return stockData_df

    def changeFields(self, stockData_df):
        stockData_df = stockData_df.withColumn('stock_type', split(col('symbol'), ":")[1]) \
                                    .withColumn('stock_symbol', split(col('symbol'), ":")[0])
        return stockData_df

    def dropFields(self, stockData_df):
        stockData_df = stockData_df.drop('pre_or_pos_market_change_percent', 'symbol', 'google_mid')
        return stockData_df

    def process(self, exploded_df):
        stockData_df = self.populateFields(exploded_df)
        stockData_df = self.changeFields(stockData_df)
        stockData_df = self.dropFields(stockData_df)
        return stockData_df