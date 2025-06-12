from pyspark.sql.window import Window
from pyspark.sql.functions import sum, col, percent_rank


def abc_classification(sales_df):
    product_sales = sales_df.groupBy("InventoryId").agg(sum("SalesDollars").alias("Revenue"))
    window = Window.orderBy(col("Revenue").desc())
    ranked = product_sales.withColumn("percentile", percent_rank().over(window))

    return ranked.withColumn("Class",
        when(col("percentile") <= 0.7, "A")
        .when(col("percentile") <= 0.9, "B")
        .otherwise("C")
    )