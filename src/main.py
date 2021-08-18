
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.types import StructType
import argparse
import findspark 
from pyspark.sql.types import *
import logging
import logging.handlers
import sys
import shutil
import os

#Setting up logging
logger = logging.getLogger('log_scope')
log_formatter = logging.Formatter("%(asctime)s - [%(levelname)s]: %(message)s")
rotating_file_handler = logging.handlers.RotatingFileHandler("logs\\log.txt",
                            maxBytes=1024*1024,
                            backupCount=2)
console_handler = logging.StreamHandler(sys.stdout) #Also log to console window

rotating_file_handler.setFormatter(log_formatter)
console_handler.setFormatter(log_formatter)

logger.addHandler(rotating_file_handler)
logger.addHandler(console_handler)

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
    if not col_name in data.columns:
        logging.getLogger('log_scope').error(f"No column named '{col_name}'.")
        raise ValueError(f"No column named '{col_name}'.")


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
        logging.getLogger('log_scope').error(f"The value '{country_column_name}' for country_column_name does not exist in the data.")
        raise ValueError(f"The value '{country_column_name}' for country_column_name does not exist in the data.")

    return data[data[country_column_name].isin(country_names)]




if __name__ == '__main__':
    # # Parse command line arguments
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--path1', type=str, required=True)
    arg_parser.add_argument('--path2', type=str, required=True)
    arg_parser.add_argument('--country', type=str, required=True, nargs='+')
    arg_parser.add_argument('--spark_path', type=str, default='C:\\Spark\\spark')
    args = arg_parser.parse_args()

    # Create Spark session
    findspark.init(args.spark_path)
    sc = SparkContext("local", "pyspark")
    spark = SparkSession.builder.getOrCreate()

    #Load datasets
    df_client = spark.read.option("header",True).csv(args.path1)
    df_financial = spark.read.option("header",True).csv(args.path2)

    # Organizing the data frames
    ## Drop personal client information
    df_client = df_client.drop('first_name')
    df_client = df_client.drop('last_name')
    df_financial = df_financial.drop('cc_n')

    #Rename columns in both dataframes
    try:
        df_client = rename_column(df_client, 'id', 'client_identifier')
        df_financial = rename_column(df_financial, 'id', 'client_identifier')
        df_financial = rename_column(df_financial, 'btc_a', 'bitcoin_address')
        df_financial = rename_column(df_financial, 'cc_t', 'credit_card_type')
    #Error, column does not exist
    except ValueError as ve:
        #Log the error and end the program
        logging.getLogger('log_scope').error('Failed to rename one a columns. Program ending early, no output will be saved.', ve.message)
        sys.exit(1)

    #Join dataframes on 'id' / 'client_identifier'
    df = df_client.join(df_financial, 'client_identifier')

    #Filter
    df_filtered = filter_country(df, args.country)

    #Clear previous saved outputs
    output_directory = 'client_data'

    if os.path.exists(output_directory):
        shutil.rmtree(output_directory)

    #Save the data
    df_filtered.write.option('header', True).csv(path=output_directory)


