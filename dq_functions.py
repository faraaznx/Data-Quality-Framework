from pyspark.sql.functions import *
from pyspark.sql.types import StringType,StructType,StructField,ArrayType,FloatType,DoubleType,DecimalType, IntegerType
import json
import re
import ast
import sys
import datetime
from pyspark.sql import SparkSession
from pyspark.sql import Window
from datetime import date, datetime

def completeness(database, table, df, completeness_cols):
    """"""

    
        # Calculating the not null percentages of every column and saving the values in a list
        

        completeness_list = [
            [
                database,
                table,
                "completeness",
                x,
                (
                    df.count()
                    - df.select(x).filter((col(x).isNull()) | (col(x) == "")).count()
                )
                * 100.0
                / df.count(),
                (df.select(x).filter((col(x).isNull()) | (col(x) == "")).count()),
                "",
                "",
            ]
            for x in completeness_cols
        ]
        for i in completeness_list:
            if i[5] != 0:
                i[6] = "The column " + str(i[3]) + " has " + str(i[5]) + " Null values"
                i[7] = "Fail"
            else:
                i[7] = "Pass"

    return completeness_list

def accuracy(df, accuracy_cols):
    return None