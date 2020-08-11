from pyspark.sql import SparkSession
import sys

schema = """`Id` INT, `First` STRING, `Last` STRING, `Url` STRING,`Published` STRING, `Hits` INT, `Campaigns` ARRAY<STRING>"""

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage mmcount file{sys.stderr}")
        sys.exit(1)

    spark = SparkSession.builder.appName("json-from-file").getOrCreate()
    authors_file = sys.argv[1]
    authors_df = spark.read.schema(schema).json(authors_file)
    print(authors_df.head(3))
    authors_df.printSchema()

