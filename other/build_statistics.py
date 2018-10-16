import sys

from pyspark.sql.functions import col
from pyspark.sql import SparkSession

dt = sys.argv[1]
print(dt)

spark = SparkSession.builder.getOrCreate()

spark.read.json(
    "gs://airflow_training_data_123/PricePaid/*/*.json"
).withColumn(
    "transfer_date", col("transfer_date").cast("timestamp").cast("date")
).createOrReplaceTempView(
    "land_registry_price_paid_uk"
)

# >>> df.printSchema()
# root
#  |-- city: string (nullable = true)
#  |-- county: string (nullable = true)
#  |-- district: string (nullable = true)
#  |-- duration: string (nullable = true)
#  |-- locality: string (nullable = true)
#  |-- newly_built: boolean (nullable = true)
#  |-- paon: string (nullable = true)
#  |-- postcode: string (nullable = true)
#  |-- ppd_category_type: string (nullable = true)
#  |-- price: double (nullable = true)
#  |-- property_type: string (nullable = true)
#  |-- record_status: string (nullable = true)
#  |-- saon: string (nullable = true)
#  |-- street: string (nullable = true)
#  |-- transaction: string (nullable = true)
#  |-- transfer_date: double (nullable = true)

# spark.read.json("gs://airflow-training-data/currency/*.json").withColumn(
#     "date", col("date").cast("date")
# ).createOrReplaceTempView("currencies")

# >>> df.printSchema()
# root
#  |-- conversion_rate: double (nullable = true)
#  |-- date: string (nullable = true)
#  |-- from: string (nullable = true)
#  |-- to: string (nullable = true)

# Do some aggregations and write it back to Cloud Storage
aggregation = spark.sql(
    """
    SELECT
        CAST(CAST(transfer_date AS timestamp) AS date) transfer_date,
        county,
        district,
        city,
        AVG(price * 1) as price,
        CAST(CAST(transfer_date AS timestamp) AS date) trans_date,
    FROM
        land_registry_price_paid_uk
    WHERE
        transfer_date = '{}'
    GROUP BY
        transfer_date,
        county,
        district,
        city,
        trans_date
    ORDER BY
        county,
        district,
        city
""".format(
        dt
    )
)

(
    aggregation.write.mode("overwrite")
    .partitionBy("transfer_date")
    .parquet("gs://airflow_training_data_123/average_prices/{}/".format(dt))
)
