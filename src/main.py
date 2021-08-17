
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.types import StructType
import argparse
import findspark 
from pyspark.sql.types import *

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


def filter_country(data, country_names, country_column_name='country'):
    """
    Filters the data frame by specified county names.

    :param data: data frame to be filtered
    :type data: pyspark.sql.dataframe.DataFrame
    :param country_names: country names to filter
    :type country_names: list[str]
    :param country_column_name: name of column in data which contains the countries
    :type country_names: str, default='country'
    :raise: ValueError if country_column_name is not in data
    :return: filtered data frame
    :rtype: pyspark.sql.dataframe.DataFrame

    """
    if not country_column_name in data.columns:
        raise ValueError(f"The value '{country_column_name}' for country_column_name does not exist in the data.")

    return data[data[country_column_name].isin(country_names)]


if __name__ == '__main__':
    # # Parse command line arguments
    # arg_parser = argparse.ArgumentParser()
    # arg_parser.add_argument('--country', type=str, required=True, nargs='+')
    # arg_parser.add_argument('--path1', type=str, required=True)
    # arg_parser.add_argument('--path2', type=str, required=True)
    # args = arg_parser.parse_args()

    # Load Spark dependencies
    findspark.init('C:\\Spark\\spark')
    sc = SparkContext("local", "pyspark")
    spark = SparkSession.builder.getOrCreate()


    #Load datasets
    path1 = "file:///C:/Users/bboyn/OneDrive/Desktop/Bitcoin User Data/dataset_one.csv"
    path2 = "file:///C:/Users/bboyn/OneDrive/Desktop/Bitcoin User Data/dataset_two.csv"

    df_client = spark.read.option("header",True).csv(path1)
    df_financial = spark.read.option("header",True).csv(path2)

    # Organizing the data frames
    ## Drop personal client information
    df_client = df_client.drop('first_name')
    df_client = df_client.drop('last_name')
    df_financial = df_financial.drop('cc_n')

    #Rename columns in both dataframes
    df_client = rename_column(df_client, 'id', 'client_identifier')
    df_financial = rename_column(df_financial, 'id', 'client_identifier')
    df_financial = rename_column(df_financial, 'btc_a', 'bitcoin_address')
    df_financial = rename_column(df_financial, 'cc_t', 'credit_card_type')


    #Join dataframes on 'id' / 'client_identifier'
    df = df_client.join(df_financial, 'client_identifier')

    #Filter
    country = ['Netherlands','United Kingdom']
    df_filtered = filter_country(df, country)
    df_filtered.write.option('header', True).csv(path='C:\\Users\\bboyn\\OneDrive\\Desktop\\Bitcoin User Data\\client_data')


