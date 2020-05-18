from pyspark.sql import SparkSession
import databricks.koalas as ks

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

    # Get context from SparkSession object.
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

    # Print collections schemas.
    df_reviews.printSchema()
    df_meta.printSchema()

    # Drop MongoDB _id column in order to avoid error at runtime.
    df_reviews = df_reviews.drop('_id')
    df_meta = df_meta.drop('_id')

    # Create two Koalas DataFrame from the Spark DataFrame objects.
    kdf_reviews = ks.DataFrame(df_reviews)
    kdf_meta = ks.DataFrame(df_meta)

    # Extract sports and outdoors data from salesRank struct.
    array_ranks = df_meta.select('salesRank.Sports &amp; Outdoors').to_koalas()
    
    # Allow merge from different DataFrame objects.
    ks.set_option('compute.ops_on_diff_frames', True)

    # Assign a new column with the array_ranks data extracted above.
    kdf_meta['sales_rank_sports_etc'] = array_ranks

    # Query 1
    kdf_1 = kdf_reviews \
        .groupby('asin') \
        .size() \
        .sort_values(ascending=False) \
        .head(100)

    print("# I 100 prodotti con il maggior numero di recensioni #")
    print(kdf_1)

    # Query #2
    kdf_2 = kdf_reviews \
        .groupby('reviewerID') \
        .size() \
        .sort_values(ascending=False) \
        .head(100)

    print("# I 100 reviewer che hanno effettuato il maggior numero di recensioni #")
    print(kdf_2)

