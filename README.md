Bitcoin User Data Exercise
==========================
This is a `PySpark` exercise for loading, transforming and joining dataframes from 
two file sources.


Introduction
------------
This application is aiming to help the company *KommatiPara* that deals with bitcoin trading.


They have two dataset; first dataset includes client information and the second dataset contains their financial 
details. Since they are planning to reach out to their clients for a new marketing push, 
the company wants a new dataset containing the emails and some financial information 
of their clients that locate in the *United Kingdom* and the *Netherlands*.

This application takes these datasets and combine them into one dataset and applies a filter for selected countries. 
It also changes the column names and removes the personal identifiable client information.


*Note: the data does not represent real client information*


Requirements
------------

[Spark](https://spark.apache.org/) >=3.0.3

[Python](https://www.python.org/) >=3.7

[pip](https://pip.pypa.io/en/stable/)


Installation Guide
------------------
First, clone the git repository using the following command:

`git clone https://github.com/bboynak/BitcoinUserData`

After that, navigate to the package directory:

`cd ./BitcoinUserData`

Next, install the required Python dependencies. It is recommended to first set up a virtual environment like so:

```
python -m venv .venv
.venv\Scripts\activate
```

The dependencies can be installed via:

`python -m pip install requirements.txt`


User Guide
----------

To run the program, use the following command:

`python src/main.py --path1 --path2 --country`

Application takes three arguments: 

* paths of the datasets: `path1`, `path2`
* list of the countries to be filtered: `country`

For example:

`python src/main.py --path1 'dataset_one.csv' --path2 'dataset_two.csv' --country Netherlands  'United Kingdom'`

