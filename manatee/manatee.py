import numpy as np
import pandas as pd
from datetime import date, datetime

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import (BooleanType, StringType, IntegerType, FloatType, Row,
                               DateType, TimestampType, StructType, StructField)


class Manatee(DataFrame):
    """
    Wrapper class around the PySpark DataFrame object, providing some usability features
    closer to the pandas.DataFrame object.
    """

    def __init__(self, df):
        """
        Initialise by setting the internal dataframe.
        """
        super(self.__class__, self).__init__(df._jdf, df.sql_ctx)


    def drop_rows(self, n=1, inplace=False):
        """
        Drop the first n rows of the dataframe.
        """

        # Add an index to the data, and remove rows where the index is less than n
        data = self.rdd.zipWithIndex().filter(lambda x: x[1] > n-1).map(lambda x: x[0])

        if inplace:
            self.__init__(data.toDF(schema=self.schema))
        else:
            return Manatee(data.toDF(schema=self.schema))


    def unique(self, column=None):
        """
        Find the unique values of a column in the dataframe, and the number of rows with that
        value. If no column is passed, this returns a dictionary, mapping column names to
        unique values in each column. This option might be slow.
        """

        # For a specified column, find unique values in that column only
        if column is not None:
            return self.groupby(column).count().collect()

        # Otherwise, find unique values in each column
        else:
            return {col: self.groupby(col).count().collect() for col in self.columns}


    def cast(self, column, dtype, inplace=False):
        """
        Attempts to cast a column to a given variable dtype. If it fails, this call "fails
        gracefully" : the DF is unchanged and Spark issues lots of text.
        """

        # Cast the column to the desired dtype
        cast_column = self[column].cast(self.typedict[dtype]())

        # Add it to the DF
        data = self.withColumn("x__", cast_column).drop(column).withColumnRenamed("x__", column)

        if inplace:
            self.__init__(data)
        else:
            return Manatee(data)


    def add_column(self, data, column=None, dtype=None, inplace=False):
        """
        Adds a DF column, or a RDD, to the dataframe. If a DataFrame ( either PySpark or Manatee )
        is passed, then column and dtype can be None, as this information is already in the
        dataframe. If data is an RDD, column must be a str specifying the desired column name,
        and dtype should be a key as found in Manatee.typedict.
        """

        # For dataframe data, just join() it
        if not isinstance(data, pyspark.rdd.RDD):
            self.__init(self.join(data))

        # For RDDs, cast to DF and then join()
        schema = StructType([
            StructField(column, self.typedict[astype]())
        ])
        column = data.toDF(schema=schema)

        if inplace:
            self.__init__(self.join(column))
        else:
            return Manatee(self.join(column))


    # Dictionary for casting columns
    typedict = {
        int: IntegerType,
        bool: BooleanType,
        float: FloatType,
        date: DateType,
        str: StringType,
        datetime: TimestampType
    }
