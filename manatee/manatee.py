"""
.. module:: manatee

.. moduleauthor:: Quentin

"""

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

    This class provides friendly methods for performing routine actions like dropping
    NAs, adding columns, casting columns to another variable type, or discovering
    unique values in a column. It aims to provide a more pandas-like experience to the
    PySpark DataFrame object.
    """

    def __init__(self, df):
        """
        Initialise the Manatee dataframe.

        Parameters
        ----------
        df : a PySpark or Manatee dataframe.
        """
        super(self.__class__, self).__init__(df._jdf, df.sql_ctx)


    def drop_rows(self, n=1, inplace=False):
        """
        Drop the first n rows of the dataframe.

        Parameters
        ----------
        n : int
            The number of rows to drop.
        inplace : bool
            If False, this method returns a new Manatee dataframe.
            If True, the current dataframe is mutated in-place, and this returns nothing.
        """

        # Add an index to the data, and remove rows where the index is less than n
        rdd = self.rdd.zipWithIndex().filter(lambda x: x[1] > n-1).map(lambda x: x[0])

        if inplace:
            self.__init__(rdd.toDF(schema=self.schema))
        else:
            return Manatee(rdd.toDF(schema=self.schema))


    def unique(self, columns=None):
        """
        Find the unique values of a column in the dataframe.

        This method returns the various unique values in a column or in the entire
        dataframe, and the number of rows with that value. If no column is passed,
        this returns a dictionary, mapping column names to unique values in each column.
        This option might be slow.

        Parameters
        ----------
        columns : str, list, or None
            The names of columns in which to find unique values.
            If columns is a string, it is the name of the column in which to find unique values.
            If columns is a list, its elements are assumed to be column names.
            If columns is None, unique values are found from across the entire dataframe,
            on a per-column basis.

        Returns
        -------
        unique_values : dict
            A dictionary whose keys are column names, and values are the unique values, and their
            counts, in that column.
        """

        if columns is None:
            columns = self.columns
        elif isinstance(columns, str):
            columns = [columns]

        return {column: self.groupby(column).count().collect() for column in columns}


    def cast(self, column, dtype, inplace=False):
        """
        Attempts to cast a column to a given variable dtype.

        If it fails, this call "fails gracefully" : the DF is unchanged and Spark issues
        lots of text. I'm not sure how to catch these errors yet...

        Parameters
        ----------
        column : str
            The name of the column whose elements are to be cast.
        dtype : type
            The type of variable to cast to. Valid entries are the keys in the dictionary
            `Manatee.typedict`. They currently are : int, float, bool, str, date, datetime.
        inplace : bool
            If False, this method returns a new Manatee DataFrame.
            If True, the current DataFrame is mutated in-place, and this method returns nothing.
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
        Adds a DF column, or a RDD, to the dataframe.

        Parameters
        ----------
        data : RDD, Pyspark DataFrame, or Manatee DataFrame.
        column : str or None.
            If `data` is a RDD, this argument species the desired column name.
            If `data` is a DataFrame, this should be None, as this information is extracted
            from the schema.
        dtype : dtype or None.
            If `data` is a RDD, this should be a variable dtype, as found in the keys of
            `Manatee.typedict`. Acceptable values are int, float, bool, str, date, or datetime.
            If data is a DataFrame, this should be None, as this information is extracted from
            the schema.
        inplace : bool
            If False, this method returns a new Manatee DataFrame.
            If True, the current DataFrame is mutated in-place, and this method returns nothing.
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


    def dropna(self, how="any", na=None, subset=None, inplace=False):
        """
        Drops rows containing NA or any of the values in na, such as na=["", "NULL"].

        Parameters
        ----------
        how : str
            If "any", drop rows that contain at least one NA element.
            If "all", drops rows only if all of their elements are NA.
        na : list or None.
            If None, only empty elements are considered NA. Otherwise, any elements in this
            list are also considered NA elements. You might want ``na = ["", "NULL"]`` to remove
            any rows containing empty elements, empty strings, and the string "NULL".
        subset : list or None.
            If None, the entire DataFrame is considered when looking for NA values.
            Otherwise, only the columns whose names are given in this argument are considered.
        inplace : bool
            If False, this method returns a new Manatee DataFrame.
            If True, the current DataFrame is mutated in-place, and this method returns nothing.
        """

        # Define the set of NA values
        if na is None:
            na = {None}
        else:
            na = set(na)

        # If we're dropping rows containing at least one NA
        if how == "any":

            # If we're looking across the full dataframe
            if not subset:
                rdd = self.rdd.filter(lambda x: not (set(x) & na))

            # If we're looking at specific columns
            else:
                rdd = self.rdd.filter(lambda x: not ({val for key, val in x.asDict().items()
                                                      if key in subset} & na))

        # Otherwise, we're only dropping rows if all entries are NA
        elif how == "all":

            # If strictly all subset column entries must be NA
            if not subset:
                rdd = self.rdd.filter(lambda x: not (set(x) <= na))

            # If we're looking at specific columns
            else:
                rdd = self.rdd.filter(lambda x: not ({val for key, val in x.asDict().items()
                                                      if key in subset} <= na))

        if inplace:
            self.__init__(rdd.toDF(schema=self.schema))
        else:
            return Manatee(rdd.toDF(schema=self.schema))







    # Dictionary for casting columns
    typedict = {
        int: IntegerType,
        bool: BooleanType,
        float: FloatType,
        date: DateType,
        str: StringType,
        datetime: TimestampType
    }
    """
    A dictionary of valid dtypes, for use when casting. This is relevant for
    `Manatee.cast` and `Manatee.add_column`.
