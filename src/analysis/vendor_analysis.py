from pyspark.sql.functions import col, avg


def analyze_vendor_efficiency(invoice_purchases_df):
    return invoice_purchases_df.groupBy("VendorNumber", "VendorName").agg(
        avg("Freight").alias("AvgFreight"),
        avg("Dollars").alias("AvgSpend")
    ).orderBy(col("AvgSpend").desc())