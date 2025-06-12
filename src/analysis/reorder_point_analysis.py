from pyspark.sql.functions import col, avg


def reorder_point(purchase_final_df):
    avg_lead_time = purchase_final_df.withColumn("lead_time", datediff("ReceivingDate", "PODate")) \
                                     .groupBy("InventoryId").agg(avg("lead_time").alias("AvgLeadTime"))

    avg_demand = purchase_final_df.groupBy("InventoryId").agg(avg("Quantity").alias("DailyDemand"))

    return avg_lead_time.join(avg_demand, "InventoryId") \
                         .withColumn("ReorderPoint", col("DailyDemand") * col("AvgLeadTime"))