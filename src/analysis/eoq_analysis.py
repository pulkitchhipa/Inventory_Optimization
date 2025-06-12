import math
from pyspark.sql.functions import col, udf
from pyspark.sql.types import DoubleType


def calculate_eoq(purchase_final_df):
    # Assuming fixed ordering cost and holding cost for demo
    ordering_cost = 50
    holding_cost_rate = 0.2

    grouped = purchase_final_df.groupBy("InventoryId").agg(
        sum("PurchasePrice").alias("Price"),
        sum("Quantity").alias("Demand")
    )

    def eoq_func(d, p):
        return math.sqrt((2 * d * ordering_cost) / (p * holding_cost_rate))

    eoq_udf = udf(eoq_func, DoubleType())
    return grouped.withColumn("EOQ", eoq_udf("Demand", "Price"))