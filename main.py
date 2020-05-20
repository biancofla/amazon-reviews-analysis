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

    # Compute join on asin attribute.
    kdf_merge = kdf_reviews.merge(kdf_meta, on='asin', how='left')

    # Query 1
    kdf_1 = kdf_reviews \
        .groupby('asin') \
        .size() \
        .sort_values(ascending=False)

    print("# I 100 prodotti con il maggior numero di recensioni #")
    print(kdf_1.head(100))

    # Query #2
    kdf_2 = kdf_reviews \
        .groupby('reviewerID') \
        .size() \
        .sort_values(ascending=False)

    print("# I 100 reviewer che hanno effettuato il maggior numero di recensioni #")
    print(kdf_2.head(100))

    # Query #3
    kdf_3 =  kdf_merge \
        [kdf_merge.brand != ''] \
        .dropna(subset=['brand']) \
        .groupby('brand') \
        .size() \
        .sort_values(ascending=False)

    print("# Le 50 marche i cui prodotti sono stati maggiormente recensiti #")
    print(kdf_3.head(50))

    # Query #4
    kdf_4 =  kdf_meta \
        .dropna(subset=['brand', 'price']) \
        .groupby('brand') \
        ['price'] \
        .mean() \
        .sort_values(ascending=False)

    print("# Le 50 marche i cui prodotti hanno un prezzo medio maggiore #")
    print(kdf_4.head(50))
    
    # Query #5
    kdf_5 = kdf_reviews \
        .groupby('asin') \
        ['overall'] \
        .mean() \
        .sort_values(ascending=False)

    kdf_5 = ks.concat([kdf_1, kdf_5], axis=1)

    kdf_5 = kdf_5 \
        .sort_values(by=['overall', 'count'], ascending=False)

    print("# I 100 prodotti con le migliori recensioni #")
    print(kdf_5.head(100))

    # Query #6
    kdf_6 = kdf_merge \
        [kdf_merge.brand != ''] \
        .dropna(subset=['brand']) \
        .groupby('brand') \
        ['overall'] \
        .mean() \
        .sort_values(ascending=False)

    kdf_6 = ks.concat([kdf_3, kdf_6], axis=1)

    kdf_6 = kdf_6 \
        .sort_values(by=['overall', 'count'], ascending=False)

    print("# Le 100 marche con le migliori recensioni #")
    print(kdf_6.head(50))
    