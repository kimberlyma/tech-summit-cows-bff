from pyspark.sql.functions import array
from pyspark.sql.dataframe import DataFrame

def identify_friend_groups(n: int, distance: DataFrame, overlap: DataFrame):
    friend_groups = distance \
        .orderBy('distance', ascending=False) \
        .filter("cow1 < cow2") \
        .limit(n) \
        .withColumn("friend_group", (array('cow1', 'cow2'))) \
        .join(overlap, on = ["cow1", "cow2"])
    
    return friend_groups