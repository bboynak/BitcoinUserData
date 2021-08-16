from pyspark import SparkContext
from pyspark.sql import SparkSession
import findspark
#Import functions from main code to be tested
from src.main import rename_column, filter_country

findspark.init('C:\\Spark\\spark')
sc = SparkContext("local", "pyspark")
spark = SparkSession.builder.getOrCreate()


def test_rename_column_columdoesnotexist():
    data = [(),(),()]
    df = spark.createDataFrame(data, schema = ['first_name', 'last_name', 'address'])
    rename_column(df, 'NOTAREALCOLUMNNAME', 'newName')

def test_filter_countrydoesnotexist():
    data = [(),(),('Netherlands', 'United Kingdom', 'Germany')]
    df = spark.createDataFrame(data, schema = ['first_name', 'last_name', 'country'])
    filter_country(df, ['Paris', 'Netherlands'])
