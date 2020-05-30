from pyspark.sql import SparkSession
import time

# Benchmark execution time.
t = time.time()

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

    def get_helpful_rate(array):
        '''
            Compute the fraction of users that found the review helpful.

            Args:
                array (array): two elements array.
                
                E.g. [1, 2], one found the review helpful over a total 
                of two. 

            Returns:
                float: percentage of user that found the review helpful.

        '''
        num = array[0]
        den = array[1]
        res = 0

        if den != 0:
            res = num/den * 100
        
        return float('%.2f' % res)

    # Create helpful_rate and helpful_pos columns from helpful column.
    df_reviews = df_reviews.rdd \
        .map(lambda x: x + (get_helpful_rate(x['helpful']), x['helpful'][0], )) \
        .toDF(df_reviews.columns + ['helpful_rate', 'helpful_pos']) \
        .drop('helpful')

    # Create sales_rank_sports_etc columns from salesRank column.
    df_meta = df_meta \
        .join(df_meta.select(['asin', 'salesRank.Sports &amp; Outdoors']), on='asin') \
        .drop('salesRank') \
        .withColumnRenamed('Sports &amp; Outdoors', 'sales_rank_sports_etc')

    # Query #1
    df_1 = df_reviews \
        .groupby('asin') \
        .count() \
        .withColumnRenamed('count', 'reviews_count_product') \
        .orderBy('reviews_count_product', ascending=False)

    print("# I 100 prodotti con il maggior numero di recensioni #")
    df_1.show(100)

    # Query #2
    df_2 = df_reviews \
        .groupby('reviewerID') \
        .count() \
        .withColumnRenamed('count', 'reviews_count_reviewer') \
        .orderBy('reviews_count_reviewer', ascending=False)

    print("# I 100 reviewer che hanno effettuato il maggior numero di recensioni #")
    df_2.show(100)

    # Query #3
    df_3 = df_reviews \
        .join(df_meta, on='asin') \
        .filter("brand != ''") \
        .dropna(how='any', subset=('brand')) \
        .groupBy('brand') \
        .count() \
        .withColumnRenamed('count', 'reviews_count_brand') \
        .orderBy('reviews_count_brand', ascending=False)

    print("# Le 50 marche i cui prodotti sono stati maggiormente recensiti #")
    df_3.show(50)

    # Query #4
    df_4 =  df_reviews \
        .join(df_meta, on='asin') \
        .filter("brand != ''") \
        .dropna(how='any', subset=('brand', 'price')) \
        .select(['brand', 'price']) \
        .groupby('brand') \
        .mean() \
        .orderBy('avg(price)', ascending=False)

    df_4 = df_4.rdd \
        .map(lambda x: x + ('%.2f' % x['avg(price)'], )) \
        .toDF(df_4.columns + ['price_mean']) \
        .drop('avg(price)')

    print("# Le 50 marche i cui prodotti hanno un prezzo medio maggiore #")
    df_4.show(50)

    # Query #5
    df_5 = df_reviews \
        .select(['asin', 'overall']) \
        .groupby('asin') \
        .mean()
    
    df_5 = df_5.rdd \
        .map(lambda x: x + ('%.2f' % x['avg(overall)'], )) \
        .toDF(df_5.columns + ['overall_mean_product']) \
        .drop('avg(overall)')

    df_5 = df_5 \
        .join(df_1, on='asin') \
        .orderBy(['overall_mean_product', 'reviews_count_product'], ascending=False)

    print("# I 100 prodotti con le migliori recensioni #")
    df_5.show(100)

    # Query #6
    df_6 = df_reviews \
        .join(df_meta, on='asin') \
        .filter("brand != ''") \
        .dropna(how='any', subset=('brand')) \
        .select(['brand', 'overall']) \
        .groupBy('brand') \
        .mean() 

    df_6 = df_6.rdd \
        .map(lambda x: x + ('%.2f' % x['avg(overall)'], )) \
        .toDF(df_6.columns + ['overall_mean_brand']) \
        .drop('avg(overall)')

    df_6 = df_6 \
        .join(df_3, on='brand') \
        .orderBy(['overall_mean_brand', 'reviews_count_brand'], ascending=False)

    print("# Le 100 marche con le migliori recensioni #")
    df_6.show(100)

    # Query #7 - #8
    df_mean = df_reviews \
        .select(['reviewerID', 'helpful_rate']) \
        .groupBy('reviewerID') \
        .mean('helpful_rate') \
        .withColumnRenamed('avg(helpful_rate)', 'helpful_rate_mean')
    
    df_sum = df_reviews \
        .select(['reviewerID', 'helpful_pos']) \
        .filter('helpful_pos != 0') \
        .groupBy('reviewerID') \
        .sum() \
        .withColumnRenamed('sum(helpful_pos)', 'helpful_pos_sum')

    df_7 = df_mean \
        .join(df_sum, on='reviewerID') \
        .orderBy(['helpful_rate_mean', 'helpful_pos_sum'], ascending=False)

    # # Query #7
    print("# I 100 reviewer che hanno effettuato recensioni con la maggiore utilità media #")
    df_7.show(100)

    # # Query #8
    print("# I 100 reviewer che hanno effettuato recensioni con la minore utilità media #")
    df_8 = df_7 \
        .orderBy(['helpful_rate_mean', 'helpful_pos_sum'], ascending=True)

    df_8.show(100)

    # Query #9
    df_9 = df_meta \
        .dropna(how='any', subset=('sales_rank_sports_etc')) \
        .orderBy('sales_rank_sports_etc', ascending=True) \
        .select(['asin', 'sales_rank_sports_etc'])

    print('# I 100 prodotti con il migliore ranking nelle vendite #')
    df_9.show(100)

    # Query #10
    df_10 = df_meta \
        .filter("brand != ''") \
        .dropna(how='any', subset=('brand', 'sales_rank_sports_etc')) \
        .select(['brand', 'sales_rank_sports_etc']) \
        .groupby('brand') \
        .mean() \
        .orderBy(['avg(sales_rank_sports_etc)'], ascending=True)

    df_10 = df_10.rdd \
        .map(lambda x: x + ('%.2f' % x['avg(sales_rank_sports_etc)'], )) \
        .toDF(df_10.columns + ['sales_rank_sports_etc_mean']) \
        .drop('avg(sales_rank_sports_etc)')

    print('# Le 50 marche i cui prodotti hanno il ranking medio migliore #')
    df_10.show(50)

    elapsed = time.time() - t
    print('Execution time (s): {}'.format(elapsed))
