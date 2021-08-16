
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.types import StructType
import argparse
import findspark 

findspark.init('C:\Spark\spark')

from pyspark.sql.types import *

sc = SparkContext("local", "pyspark")
spark = SparkSession.builder.getOrCreate()

#Arguments
# p = argparse.ArgumentParser()

# p.add_argument('country', required=True)
# p.add_argument('path1', required=True)
# p.add_argument('path2', required=True)

#Importing data

# schema_1 = StructType([
#     StructField("client_identifier", IntegerType(),True), 
#     StructField("first_name", StringType(),True),
#     StructField("last_name", StringType(),True), 
#     StructField("email", StringType(),True),
#     StructField("Country", StringType(),True)])

# schema_2 = StructType([
#     StructField("client_identifier", IntegerType(),True), 
#     StructField("bitcoin_address", StringType(),True),
#     StructField("credit_card_type", StringType(),True), 
#     StructField("credit_card_number", StringType(),True)])


path1 = "file:///C:/Users/bboyn/OneDrive/Desktop/Bitcoin User Data/dataset_one.csv"
path2 = "file:///C:/Users/bboyn/OneDrive/Desktop/Bitcoin User Data/dataset_two.csv"

df_client = spark.read.option("header",True).csv(path1)
df_financial = spark.read.option("header",True).csv(path2)

# Organizing the data frames
## Drop personal client information
df_client = df_client.drop('first_name')
df_client = df_client.drop('last_name')
df_financial = df_financial.drop('cc_n')

#Renaming the columns
def rename_column(data, col_name, new_col_name):
    """ 
    Renaming a column in a data frame.

    :param data: data frame 
    :type data: spark.sql.dataframe.DataFrame
    :param col_name: column name to be renamed
    :type col_name: str
    :param new_col_name: new column name
    :type new_col_name: str
    :raise: ValueError if col_name is not in data
    :return: data frame with the renamed column
    :rtype: spark.sql.dataframe.DataFrame

    """
    return data.withColumnRenamed(col_name, new_col_name)

df_client = rename_column(df_client, 'id', 'client_identifier')
df_financial = rename_column(df_financial, 'id', 'client_identifier')
df_financial = rename_column(df_financial, 'btc_a', 'bitcoin_address')
df_financial = rename_column(df_financial, 'cc_t', 'credit_card_type')



#Join dataframes on 'id' / 'client_identifier'
df = df_client.join(df_financial, 'client_identifier')

#Filter

def filter_country(data, country_names):
    """
    Filters the data frame by specified county names.

    :param data: data frame to be filtered
    :type data: pyspark.sql.dataframe.DataFrame
    :param country_names: country names to filter
    :type country_names: list[str]
    :raise: ValueError if country_name is not in data
    :return: filtered data frame
    :rtype: pyspark.sql.dataframe.DataFrame

    """
    return data[data.country.isin(country_names)]


country = ['Netherlands','United Kingdom']
df_filtered = filter_country(df, country)
df_filtered.show()
