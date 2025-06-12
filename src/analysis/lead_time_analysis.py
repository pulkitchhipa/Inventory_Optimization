from pyspark.sql.functions import datediff, col, avg


def analyze_lead_time(purchase_final_df):
    return purchase_final_df.withColumn("lead_time", datediff("ReceivingDate", "PODate")) \
                             .groupBy("VendorNumber", "VendorName") \
                             .agg(avg("lead_time").alias("AvgLeadTime"))