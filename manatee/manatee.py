"""
.. module:: manatee

.. moduleauthor:: Quentin

"""

import numpy as np
import pandas as pd
from datetime import date, datetime

from pyspark.rdd import RDD
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

    Class Attributes
    ----------------
    typedict : dict
        A dictionary of valid dtypes, for use when casting. This is relevant for
        `Manatee.quick_cast` and `Manatee.add_column`.
    """

    def __init__(self, df=None):
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

        # Format columns into a list of column names
        if columns is None:
            columns = self.columns
        elif isinstance(columns, str):
            columns = [columns]

        return {column: self.groupby(column).count().collect() for column in columns}


    def quick_cast(self, column, dtype, inplace=False):
        """
        Attempts to cast a column to a given variable dtype.

        Unlike the PySpark DataFrame `cast` method, `quick_cast` doesn't return a column
        separately, but rather casts a column inside the DataFrame, keeping the column's
        name but changing the dtype of its elements. If it fails, this call "fails gracefully" :
        the DataFrame is unchanged and Spark issues lots of text.
        I'm not sure how to catch these exceptions yet...

        Parameters
        ----------
        column : str
            The name of the column whose elements are to be cast.
        dtype : type
            The type of variable to cast to. Valid entries are the keys in the dictionary
            `Manatee.typedict`.
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
        data : scalar, RDD, Pyspark DataFrame, or Manatee DataFrame.
            If scalar, it is broadcast to the correct length. This must fit in memory.
            If RDD, you must specify `column` and `dtype`.
        column : str or None.
            If `data` is a scalar or RDD, this argument species the desired column name.
            If `data` is a DataFrame, this is ignored, as this is extracted from the schema.
        dtype : dtype or None.
            If `data` is a scalar or RDD, this should be a variable dtype, as found in the keys
            of `Manatee.typedict`.
            If data is a DataFrame, this is ignored, as this is extracted from the schema.
        inplace : bool
            If False, this method returns a new Manatee DataFrame.
            If True, the current DataFrame is mutated in-place, and this method returns nothing.
        """

        # If it's not a type of dataframe, it's either a scalar or RDD
        if not isinstance(data, Manatee) or not isinstance(data, DataFrame):

            # Broadcast any scalars to RDD
            if not isinstance(data, RDD):
                astype = type(data)
                data = sc.parallelize([data] * self.count())

            # At this point, we have an RDD. Generate a DataFrame and add it.
            df = Manatee.from_rdd(data, dtype)

        # We now have a single-column DataFrame; join it to the existing DataFrame
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
            If None, only empty elements are considered NA. Otherwise, all elements in this
            list are also considered NA elements. You might want ``na = ["", "NULL"]`` to remove
            any rows containing empty elements, empty strings, and the string "NULL".
        subset : str, list, or None.
            If None, the entire DataFrame is considered when looking for NA values.
            Otherwise, only the columns whose names are given in this argument are considered.
        inplace : bool
            If False, this method returns a new Manatee DataFrame.
            If True, the current DataFrame is mutated in-place, and this method returns nothing.
        """

        # Define the set of NA values
        if na is None:
            na = {na}
        else:
            na = set(na).union({None})

        # Insert the subset into a list if it's passed as a string
        if isinstance(subset, str):
            subset = [subset]

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


    def from_rdd(self, rdd, name=None):
        """
        Create a Manatee DataFrame from an RDD.

        This method returns a Manatee DataFrame from data in a resilient distributed dataset.

        Parameters
        ----------
        rdd : pyspark.rdd.RDD
            The RDD to be turned into a DataFrame. Elements of the RDD can either be
            single non-container items ( like a float or str ), a tuple of such elements,
            or a `pyspark.sql.types.Row` object.
        name : str or list
            The desired name(s) of the column(s), if the RDD's elements are not `Row` objects.
        """

        # Grab the first row in the RDD, to figure out dtypes.
        first = rdd.first()

        # For non-Row RDD entries, find the dtypes assuming they're consistent throughout
        if not isinstance(first, Row):
            if isinstance(first, tuple):  # multiple elements per RDD row
                dtype = [type(i) for i in first]
            else:
                dtype = [type(first)]  # one element per RDD row

            # Also generate some names if they aren't passed
            if name is None:
                name = ["col_from_rdd_{}".format(i) for i in range(len(first))]

        # For Row RDD entries, map the first row to a dict to get dtypes and column names
        else:
            first = first.asDict()
            dtype = [type(i) for i in first.values()]
            name = [colname for i in first.keys()]

        # Create schema
        schema = StructType([
            StructField(colname, self.typedict[coltype]())
            for colname, coltype in zip(name, dtype)
        ])

        return Manatee(rdd.toDF(schema=schema))


    def transpose(self, memory=False, inplace=True):
        """

        """

        # For in-memory transpose, just go through pandas
        if memory:
            df = Manatee(df.sql_ctx.createDataFrame(df.toPandas().T))

        # Otherwise, take the RDD row by row, turning them into columns
        def rddTranspose(rdd):
            rddT1 = rdd.zipWithIndex().flatMap(lambda (x,i): [(i,j,e) for (j,e) in enumerate(x)])
            rddT2 = rddT1.map(lambda (i,j,e): (j, (i,e))).groupByKey().sortByKey()
            rddT3 = rddT2.map(lambda (i, x): sorted(list(x), cmp=lambda (i1,e1),(i2,e2) : cmp(i1, i2)))
            rddT4 = rddT3.map(lambda x: map(lambda (i, y): y , x))
            return rddT4.map(lambda x: np.asarray(x))
        # http://www.dotnetperls.com/lambda-python
        #TODO


    @property
    def T(self):
        """
        Transpose the DataFrame's index and columns fully in-memory.

        This calls ``df.transpose(memory=True, inplace=False)`` to quickly transpose the
        RDD, assuming the whole thing can fit into memory. For a transpose that only loads
        one row into memory at once, use ``Manatee.transpose()`` with ``memory=False``.
        """

        return self.transpose(memory=True, inplace=inplace)


    def na_rate(self, na=None, subset=None):
        """
        Returns the fraction of NA elements per column.
        """

        # Define the set of NA values
        if na is None:
            na = {na}
        else:
            na = set(na).union({None})

        length = float(self.count())

        nulls = self.map(lambda x: [1 if y in na else 0 for y in x]).toDF(df.schema)
        # TODO
        pass


    def apply(self, f):
        # TODO
        pass



    def toPySparkDF(self):
        """
        Returns a PySpark DataFrame object.

        Returns
        -------
        df : pyspark.sql.dataframe.DataFrame
        """
        return DataFrame(self._jdf, self.sql_ctx)


    def join(self, *args, **kwargs):
        """
        Overloads PySpark DataFrame's `join` method to return a Manatee DataFrame.
        https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame
        """

        return Manatee(super(self.__class__, self).join(*args, **kwargs))


    def select(self, *args, **kwargs):
        """
        Overloads PySpark DataFrame's `select` method to return a Manatee DataFrame.
        https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame
        """

        return Manatee(super(self.__class__, self).select(*args, **kwargs))


    def agg(self, *args, **kwargs):
        """
        Overloads PySpark DataFrame's `agg` method to return a Manatee DataFrame.
        https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame
        """

        return Manatee(super(self.__class__, self).agg(*args, **kwargs))




    # Dictionary for casting columns
    typedict = {
        int: IntegerType,
        bool: BooleanType,
        float: FloatType,
        date: DateType,
        str: StringType,
        datetime: TimestampType
    }
