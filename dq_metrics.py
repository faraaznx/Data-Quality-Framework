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

def completeness(df, completeness_cols):
    """ Space for docstring"""


    # Calculating the not null percentages of every column and saving the values in a list

    completeness_list = [
        [
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
        if i[2] != 0:
            i[4] = "The column " + str(i[3]) + " has " + str(i[2]) + " Null values"
            i[5] = "Fail"
        else:
            i[5] = "Pass"

    return completeness_list

def accuracy(df, accuracy_cols, regex):
    accuracy_list = [
        [
            (
                df.count()
                - (
                    df.select(col(x))
                    .filter(col(x).isin(regex) | col(x).rlike(regex))
                    .count()
                )
            )
            * 100
            / df.count()
        ]
        for x in accuracy_cols
    ]
    return accuracy_list

def integrity(df_f, df_d, int_cols):
    return None