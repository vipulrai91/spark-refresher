# /home/vipul/anaconda3/envs/ml/bin/python /home/vipul/Work/Github/spark-refresher/learning_spark_v2/mnm.py "data/mnm_dataset.csv"
# spark-submit learning_spark_v2/mnm.py data/mnm_dataset.csv
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import count


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage mmcount file{sys.stderr}")
        sys.exit(1)

    spark = SparkSession.builder.appName("PythonMnMCount").getOrCreate()
    mnm_file = sys.argv[1]
    mnm_DF = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(mnm_file)
    )

    print(mnm_DF.head(5))

    count_mnm_df = (
        mnm_DF.select("State", "Color", "Count")
        .groupBy("State", "Color")
        .agg(count("Count").alias("Total"))
        .orderBy("Total", ascending=False)
    )

    count_mnm_df.show(n=60, truncate=False)
    print("Total Rows = %d" % (count_mnm_df.count()))

    ca_count_mnm_df = (
        mnm_DF.select("State", "Color", "Count")
        .where(mnm_DF.State == "CA")
        .groupBy("State", "Color")
        .agg(count("Count").alias("Total"))
        .orderBy("Total", ascending=False)
    )

    ca_count_mnm_df.show(n=10, truncate=False)
    print("Total Rows = %d" % (ca_count_mnm_df.count()))

    spark.stop()
