from pyspark.sql.functions import to_date, col, date_trunc, sum

def forecast_demand(sales_df):
    # Monthly demand per product
    df = sales_df.withColumn("SalesDate", to_date("SalesDate"))
    monthly = df.groupBy(date_trunc("month", "SalesDate").alias("Month"), "InventoryId") \
               .agg(sum("SalesQuantity").alias("TotalSales"))

    # Forecast next month as simple moving average (last 3 months)
    windowed = monthly.groupBy("InventoryId").agg({"TotalSales": "avg"}).withColumnRenamed("avg(TotalSales)", "Forecast")
    return windowed