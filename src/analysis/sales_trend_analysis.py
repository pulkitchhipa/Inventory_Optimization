from pyspark.sql.functions import to_date, year, month, sum

def analyze_sales_trend(sales_df):
    df = sales_df.withColumn("SalesDate", to_date("SalesDate"))
    trend = df.groupBy(year("SalesDate").alias("Year"), month("SalesDate").alias("Month")) \
              .agg(sum("SalesDollars").alias("MonthlySales"))
    return trend.orderBy("Year", "Month")