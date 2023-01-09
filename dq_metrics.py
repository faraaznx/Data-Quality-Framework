from pyspark.sql.functions import col


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
            i[4] = "The column " + str(i[1]) + " has " + str(i[3]) + " Null values"
            i[5] = "Fail"
        else:
            i[5] = "Pass"

    return spark.createDataFrame(
        completeness_list,
        [
            "metric",
            "column_name",
            "metric value (%)",
            "faulty_records_count",
            "comment",
            "status",
        ],
    )

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
    for i in accuracy_list:
        if i[2] != 0:
            i[4] = "The column " + str(i[1]) + " has " + str(i[3]) + " inaccurate values"
            i[5] = "Fail"
        else:
            i[5] = "Pass"
    return spark.createDataFrame(
        accuracy_list,
        [
            "metric",
            "column_name",
            "metric value (%)",
            "faulty_records_count",
            "comment",
            "status",
        ],
    )

def integrity(df_f, df_d, d_col, f_col):
    # This can only take one set of integrity arg. What if i have multiple cols to check integrity and they link to more than 2 tables
    row_counts = df_f.count()
    i_list = [
        "integrity",
        str(f_col),
        0,
        df_f.select(col(f_col)).distinct().exceptAll(df_d.select(col(d_col))).count(),
        "",
        "",
    ]
    i_list[2] = (row_counts - i_list[3]) * 100 / row_counts
    if i_list[3] != 0:
        i_list[4] = (
            "The column "
            + i_list[1]
            + " has "
            + str(i_list[3])
            + " values that are not present in its parent table."
        )
    return i_list