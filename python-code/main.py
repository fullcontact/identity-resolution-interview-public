from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, explode, concat_ws, split, col, collect_set
from pyspark.sql.types import ArrayType, StringType
import os

spark = SparkSession \
    .builder \
    .master("local[3]")\
    .appName("FullContact_JingScribner") \
    .config("spark.ui.enabled", "false") \
    .getOrCreate()
sc = spark.sparkContext

# a udf to change the format of an array
join_udf = udf(lambda x: " ".join(x))
project_dir = os.path.dirname(os.path.abspath(__file__))

def parse_to_array (line):
    """
    parse rdd's format into array format
    """
    fields = line.split(" ")
    return [fields]

def id_relationship_fact (df_a, df_b):
    """
    create a relationship dataframe based on df_a left join to df_b
    return a datframe result ids from df_a and the df_b corresponding recrods
    """
    relationship_df = df_a.join(df_b, on="identity_id", how='left').\
        na.drop(subset=["group_ids"])
    return relationship_df

def dedup_regroup (df_input):
    df_output = df_input.\
                groupby("identity_id").\
                agg(collect_set(df_input.identity_id_dedup).\
                alias('dedup_related_ids'))
    return df_output

def format_output (df_ouput, file_name):
    file_name = df_ouput.\
            select(concat_ws(':', df_ouput.identity_id, \
                df_ouput.related_ids).alias(f'file_name'))
    return file_name

# ****************#
# Read Both Files #
# ****************#  

# read both files and store data in dataframe and ddl(need to do some transformations)
queries_df = spark.read.text(
    os.path.join(project_dir, 'Queries.txt')).\
        withColumnRenamed("value","identity_id")
records_rdd = sc.textFile(
    os.path.join(project_dir, 'Records.txt'))
records_df = records_rdd.map(parse_to_array).toDF().withColumnRenamed("_1","group_ids")

# ***********************************************************************#
# logics to determine the id from queries_df with records they appear in #
# ***********************************************************************#  

# break down the array elements to join with queries_df to see if they overlap
records_df_rel = records_df.select("group_ids", explode("group_ids").\
    alias("identity_id"))
# create a relationship fact dataframe based on queries_df
relationship_fact_df = id_relationship_fact(queries_df, records_df_rel)

# ************************************************#
# Output 1 - Have relationships - keep duplicates #
# ************************************************#

# created a column with the right output format - strings delimited by " " vs array
relationship_fact_df = relationship_fact_df.\
                        withColumn("related_ids", join_udf(col("group_ids")))
output1 = format_output(relationship_fact_df, "output1")
# 32,314 rows
output1.write.format('text').mode('overwrite').\
    save(os.path.join(project_dir,'output1.txt'))

# *****************#
# Output 2 - Dedup #
# *****************#

# flatten the array first
relationship_fact_df_rel = relationship_fact_df.\
                            select("identity_id", explode("group_ids").\
                                alias("identity_id_dedup"))
relationship_fact_dedup_df = dedup_regroup(relationship_fact_df_rel)
relationship_fact_dedup_df = relationship_fact_dedup_df.\
                                withColumn("dedup_related_ids", 
                                join_udf(col("dedup_related_ids")))

output2 = format_output(relationship_fact_df, "output2")
output2.write.format('text').mode('overwrite').\
    save(os.path.join(project_dir, 'output2.txt'))

spark.stop()
sc.stop()
