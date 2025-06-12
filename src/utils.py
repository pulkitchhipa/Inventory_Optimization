from pyspark.sql.functions import col, to_date

def parse_dates(df, column):
    return df.withColumn(column, to_date(col(column), "yyyy-MM-dd"))