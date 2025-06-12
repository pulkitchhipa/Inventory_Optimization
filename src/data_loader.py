import kagglehub
from kagglehub import KaggleDatasetAdapter
from pyspark.sql import SparkSession

def get_spark():
    return SparkSession.builder \
        .appName("SloozeInventoryOptimization") \
        .getOrCreate()

def load_data():
    spark = get_spark()

    def read_csv(file):
        df = kagglehub.dataset_load(
            KaggleDatasetAdapter.PANDAS,
            "sloozecareers/slooze-challenge",
            f"slooze_challenge/{file}"
        )
        return spark.createDataFrame(df)

    return {
        "sales": read_csv("SalesFINAL12312016.csv"),
        "begin_inventory": read_csv("BegInvFINAL12312016.csv"),
        "end_inventory": read_csv("EndInvFINAL12312016.csv"),
        "purchase_2017": read_csv("2017PurchasePricesDec.csv"),
        "purchase_final": read_csv("PurchasesFINAL12312016.csv"),
        "invoice_purchases": read_csv("InvoicePurchases12312016.csv")
    }