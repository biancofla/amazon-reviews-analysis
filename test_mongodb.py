from pyspark.sql import SparkSession
from pyspark.sql import functions as func

uri_db = 'mongodb+srv://<username>:<password>@bigdata.toqh2.mongodb.net'
spark_connector_uri = 'org.mongodb.spark:mongo-spark-connector_2.11:2.2.7'

if __name__ == '__main__':
    # Create a SparkSession object.
    session = SparkSession.builder \
        .master('local') \
        .appName('big-data-final-exam') \
        .config('spark.mongodb.input.uri', uri_db) \
        .config('spark.jars.packages', spark_connector_uri) \
        .getOrCreate()

    context = session.sparkContext

    # Read data from MongoDB and return two DataFrame objects, one
    # for each collection contained in database.
    df_reviews = session.read \
        .format('com.mongodb.spark.sql.DefaultSource') \
        .option('database', 'test') \
        .option('collection', 'reviews') \
        .load()
    df_meta = session.read \
        .format('com.mongodb.spark.sql.DefaultSource') \
        .option('database', 'test') \
        .option('collection', 'meta') \
        .load()

    # Print schemas.
    df_reviews.printSchema()
    df_meta.printSchema()

    # Drop MongoDB _id column in order to avoid error at runtime.
    df_reviews = df_reviews.drop('_id')
    df_meta = df_meta.drop('_id')

    # Join dataframes on asin attribute.
    df_result = df_reviews.join(df_meta, on=['asin'], how='inner')
    