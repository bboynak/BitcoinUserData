from pyspark import SparkContext
from pyspark.sql import SparkSession
import findspark
import pytest
#Import functions from main code to be tested
from src.main import rename_column, filter_country

findspark.init('C:\\Spark\\spark')
sc = SparkContext("local", "pyspark")
spark = SparkSession.builder.getOrCreate()

def test_rename_column_columdoesnotexist():
    #Arrange
    data = [('bill', 'billy', 'address1'),('sam', 'sammy', 'address2')]
    df = spark.createDataFrame(data, schema = ['first_name', 'last_name', 'address'])
    col_name = 'NOTAREALCOLUMNNAME'
    #Act
    with pytest.raises(ValueError) as column_exception:
        rename_column(df, col_name, 'newName')

    #Assert
    assert str(column_exception.value) == f"No column named '{col_name}'."
    
def test_rename_column_happy_test():
    #Arrange
    data = [('bill', 'billy', 'address1'),('sam', 'sammy', 'address2')]
    df = spark.createDataFrame(data, schema = ['first_name', 'last_name', 'address'])

    #Act
    df_renamed = rename_column(df, 'address', 'new_name')
    #Assert
    assert 'new_name' in df_renamed.columns
    assert not 'address' in df_renamed.columns

def test_filter_country_chosen_country_not_in_data():
    #Arrange 
    data = [('george', 'wilburg', 'Germany'),('tom', 'None', 'Netherlands'),('None', 'None', 'United Kingdom')]
    df = spark.createDataFrame(data, schema = ['first_name', 'last_name', 'country'])
    
    #Act
    filtered_df = filter_country(df, ['Paris'])

    #Assert
    assert filtered_df.count() == 0
    
def test_filter_country_country_column_name_not_exists():
    #Arrange 
    data = [('george', 'wilburg', 'Germany'),('tom', 'None', 'Netherlands'),('None', 'None', 'United Kingdom')]
    df = spark.createDataFrame(data, schema = ['first_name', 'last_name', 'country'])
    
    country_column_name = 'NOTAREALCOLUMN'
    with pytest.raises(ValueError) as column_exception:
        filter_country(df, ['Paris'], country_column_name)

    assert str(column_exception.value) == f"The value '{country_column_name}' for country_column_name does not exist in the data."
    
def test_filter_country_happy_test():
    #Arrange 
    data = [('george', 'wilburg', 'Germany'),('tom', 'None', 'Netherlands'),('None', 'None', 'United Kingdom')]
    df = spark.createDataFrame(data, schema = ['first_name', 'last_name', 'country'])
    
    #Act
    filtered_df = filter_country(df, ['Germany'])

    #Assert
    assert filtered_df.count() == 1
    assert filtered_df.first()['country'] == 'Germany'