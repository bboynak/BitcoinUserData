from src.main import rename_column
from pyspark import SparkContext
from pyspark.sql import SparkSession
sc = SparkContext("local", "pyspark")
spark = SparkSession.builder.getOrCreate()

# def test_rename_column_columexists():


def test_rename_column_columdoesnotexist():
    data = [(),(),()]
    df = spark.createDataFrame(data, schema = ['first_name', 'last_name', 'address'])
    rename_column(df, 'NOTAREALCOLUMNNAME', 'newName')

def test_filter_countrydoesnotexist():
    data = [(),(),('Netherlands', 'United Kingdom', 'Germany')]
    df = spark.createDataFrame(data, schema = ['first_name', 'last_name', 'country'])
    filter_country(df, ['Paris', 'Netherlands'])


def func(x):
    return x + 1

def test_answer():
    assert func(3) == 5