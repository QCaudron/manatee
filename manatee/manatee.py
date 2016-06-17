"""
.. module:: manatee

.. moduleauthor:: Quentin

"""

from datetime import date, datetime

from pyspark.rdd import RDD
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import (BooleanType, StringType, IntegerType, FloatType, Row,
                               DateType, TimestampType, StructType, StructField,
                               NullType, LongType)
from pyspark.sql.functions import monotonically_increasing_id


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
    typedict : dict.
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


    def drop_first(self, n=1, inplace=False):
        """
        Drop the first n rows of the dataframe.

        Parameters
        ----------
        n : int.
            The number of rows to drop.
        inplace : bool.
            If False, this method returns a new Manatee dataframe.
            If True, the current dataframe is mutated in-place, and this returns nothing.
        """

        # Add an index to the data, and remove rows where the index is less than n
        rdd = self.rdd.zipWithIndex().filter(lambda x: x[1] > n - 1).map(lambda x: x[0])

        if inplace:
            self.__init__(rdd.toDF(schema=self.schema))
        else:
            return Manatee(rdd.toDF(schema=self.schema))


    def drop_last(self, n=1, inplace=False):
        """
        Drop the last n rows of the dataframe.

        Parameters
        ----------
        n : int.
            The number of rows to drop.
        inplace : bool.
            If False, this method returns a new Manatee dataframe.
            If True, the current dataframe is mutated in-place, and this returns nothing.
        """

        # Total rows in the dataframe
        rows = self.count()

        # Add an index to the data, and remove rows where the index is greater than rows - n
        rdd = self.rdd.zipWithIndex().filter(lambda x: x[1] < rows - n).map(lambda x: x[0])

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
        columns : str, list, or None.
            The names of columns in which to find unique values.
            If columns is a string, it is the name of the column in which to find unique values.
            If columns is a list, its elements are assumed to be column names.
            If columns is None, unique values are found from across the entire dataframe,
            on a per-column basis.

        Returns
        -------
        unique_values : dict.
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
        separately, but rather casts a column inside the dataframe, keeping the column's
        name but changing the dtype of its elements. If it fails, this call "fails gracefully" :
        the dataframe is unchanged and Spark issues lots of text.
        I'm not sure how to catch these exceptions yet...

        Parameters
        ----------
        column : str.
            The name of the column whose elements are to be cast.
        dtype : type.
            The type of variable to cast to. Valid entries are the keys in the dictionary
            `Manatee.typedict`.
        inplace : bool.
            If False, this method returns a new Manatee DataFrame.
            If True, the current dataframe is mutated in-place, and this method returns nothing.
        """

        # Cast the column to the desired dtype
        cast_column = self[column].cast(self.typedict[dtype]())

        # Add it to the DF
        data = self.withColumn("x__", cast_column).drop(column).withColumnRenamed("x__", column)

        if inplace:
            self.__init__(data)
        else:
            return Manatee(data)


    def concatenate(self, data, inplace=False):
        """
        Concatenates a dataframe, a RDD, or a scalar column to the current dataframe.

        Parameters
        ----------
        data : scalar, RDD, PySpark DataFrame, or Manatee DataFrame.
            If dataframe, this glues the two dataframes together columnwise. They must have the
            same length, and column names must be unique.
            If RDD, it must be able to generate a Manatee DataFrame using `Manatee.from_rdd()`.
            If scalar, `data` must be either a int, a float, or a str. The scalar will be
            broadcast such that all elements in the new column have the value `data`.
        inplace : bool.
            If False, this method returns a new Manatee DataFrame.
            If True, the current DataFrame is mutated in-place, and this method returns nothing.
        """

        # If we have a dataframe, we can easily grab the schema then extract the RDD
        if isinstance(data, (Manatee, DataFrame)):
            right_schema = StructType([StructField("Manatee_idx", LongType(), False)] +
                                     data.schema.fields)
            data = data.rdd

        # Otherwise, we need to infer the schema
        elif isinstance(data, RDD):
            first = data.first().asDict()
            dtype = [type(value) for value in first.values()]
            name = [colname for colname in first.keys()]

            right_schema = StructType([StructField("Manatee_idx", LongType(), False)] +
                [StructField(colname, cls.typedict[coltype]())
                for colname, coltype in zip(name, dtype)])

        # Otherwise, it's a scalar
        elif isinstance(data, (str, float, int)):
            #rdd = sc.parallelize([data] * self.count())
            raise NotImplementedError

        # Create new schema for the current dataframe
        left_schema = StructType([StructField("Manatee_idx", LongType(), False)] +
                                 self.schema.fields)

        # Add indices, then cast to dataframes
        left = self.rdd.zipWithIndex().map(lambda (row, idx):
            {k: v for k, v in row.asDict().items() + [("Manatee_idx", idx)]}).toDF(left_schema)
        right = data.zipWithIndex().map(lambda (row, idx):
            {k: v for k, v in row.asDict().items() + [("Manatee_idx", idx)]}).toDF(right_schema)

        # Join
        df = left.join(right, "Manatee_idx").drop("Manatee_idx")

        if inplace:
            self.__init__(df)
        else:
            return Manatee(df)


    def dropna(self, how="any", na=None, subset=None, inplace=False):
        """
        Drops rows containing NA or any of the values in na, such as ``na=["", "NULL"]``.

        Parameters
        ----------
        how : str.
            If "any", drop rows that contain at least one NA element.
            If "all", drops rows only if all of their elements are NA.
        na : list or None.
            If None, only empty elements are considered NA. Otherwise, all elements in this
            list are also considered NA elements. You might want ``na = ["", "NULL"]`` to remove
            any rows containing empty elements, empty strings, and the string "NULL".
        subset : str, list, or None.
            If None, the entire dataframe is considered when looking for NA values.
            Otherwise, only the columns whose names are given in this argument are considered.
        inplace : bool.
            If False, this method returns a new Manatee DataFrame.
            If True, the current dataframe is mutated in-place, and this method returns nothing.
        """

        # Define the set of NA values
        if na is None:
            na = {None}
        elif isinstance(na, (list, set)):
            na = set(na).union({None})
        else:
            na = {na, None}

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


    @classmethod
    def from_rdd(cls, rdd, name=None):
        """
        Create a Manatee DataFrame from an RDD.

        This method returns a Manatee DataFrame from data in a resilient distributed dataset.

        Parameters
        ----------
        rdd : pyspark.rdd.RDD.
            The RDD to be turned into a dataframe. Elements of the RDD can either be
            single non-container items ( like a float or str ), a tuple of such elements,
            or a `pyspark.sql.types.Row` object.
        name : str or list.
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
            dtype = [type(value) for value in first.values()]
            name = [colname for colname in first.keys()]

        # Create schema
        schema = StructType([
            StructField(colname, cls.typedict[coltype]())
            for colname, coltype in zip(name, dtype)
        ])

        return cls(rdd.toDF(schema=schema))


    def transpose(self, memory=False, inplace=True):
        """
        Performs a transpose of the underlying RDD.

        This method is not yet fully implemented; it currently works in-memory only
        by leveraging ``pandas.DataFrame.transpose()``.

        Parameters
        ----------
        memory : bool.
            If True, the transpose is done in-memory.
            If False, the transpose is performed in a distributed fashion. At least
            one row of the dataframe must fit into memory.
        inplace : bool.
            If False, this method returns a new Manatee dataframe.
            If True, the current dataframe is mutated in-place, and this returns nothing.
        """

        # For in-memory transpose, just go through pandas
        if memory:
            df = Manatee(df.sql_ctx.createDataFrame(df.toPandas().T))
        else:
            raise NotImplementedError

        # Otherwise, take the RDD row by row, turning them into columns
        """
        def rddTranspose(rdd):
            rddT1 = self.rdd.zipWithIndex().flatMap(lambda (x,i): [(i,j,e) for (j,e) in enumerate(x)])
            rddT2 = rddT1.map(lambda (i,j,e): (j, (i,e))).groupByKey().sortByKey()
            rddT3 = rddT2.map(lambda (i, x): sorted(list(x), cmp=lambda (i1,e1),(i2,e2) : cmp(i1, i2)))
            rddT4 = rddT3.map(lambda x: map(lambda (i, y): y , x))
            return rddT4.map(lambda x: np.asarray(x))
        # http://www.dotnetperls.com/lambda-python
        """
        # TODO : finish this method

        if inplace:
            self.__init__(df)
        else:
            return Manatee(df)


    @property
    def T(self):
        """
        Transposes the dataframe's index and columns in-memory.

        This calls ``df.transpose(memory=True, inplace=False)`` to quickly transpose the
        RDD, assuming the whole thing can fit into memory. For a transpose that only loads
        one row into memory at once, use ``Manatee.transpose()`` with ``memory=False``.
        """

        return self.transpose(memory=True, inplace=False)


    def to_null(self, to_replace, subset=None, inplace=False):
        """
        Replaces a set of values with `None`.

        This method replaces a set of values with `None`, which is helpful if your
        dataframe has a number of different values that should be null, such as
        empty strings, whitespace, or the string "NULL".

        Parameters
        ----------
        to_replace : scalar or list.
            The value, or list of values, to replace with `None`.
        subset : str, list, or None.
            If None, the entire dataframe is considered when looking for to_replace values.
            Otherwise, only the columns whose names are given in this argument are considered.
        inplace : bool.
            If False, this method returns a new Manatee DataFrame.
            If True, the current dataframe is mutated in-place, and this method returns nothing.
        """

        # Ensure subset is a list of columns
        if isinstance(subset, str):
            subset = [subset]
        if subset is None:
            subset = self.columns

        # Replace a value if it's equal to, or contained in, to_replace
        if isinstance(to_replace, str):
            replace = udf(lambda x: None if x == to_replace else x)
        else:
            replace = udf(lambda x: None if x in to_replace else x)

        # Initialise a new dataframe
        df = Manatee(self)

        # Replace all matching entries in the relevant columns with None
        # TODO : use new .apply() ?!
        raise NotImplementedError

        if inplace:
            self.__init__(df)
        else:
            return Manatee(df)


    def na_rate(self, na=None, subset=None):
        """
        Returns the fraction of NA elements per column. Not yet implemented.
        """

        # Define the set of NA values
        if na is None:
            na = {na}
        else:
            na = set(na).union({None})

        length = float(self.count())

        nulls = self.map(lambda x: [1 if y in na else 0 for y in x]).toDF(df.schema)
        # TODO

        raise NotImplementedError


    def apply(self, f):
        # TODO
        for col in subset:
            df = df.withColumn(col, replace(col))

        raise NotImplementedError


    def drop(self, columns, inplace=False):
        """
        Returns a new dataframe that drops the specified column.

        This function extends PySpark's DataFrame.drop() by allowing `column` to be
        a list or tuple. In this case, all columns in the list or tuple are dropped.
        This function can also be run in-place.

        Parameters
        ----------
        column : str, list, or tuple.
            If str, drops the column named in `column`.
            If list of str or tuple of str, drops all columns named in `column`.
        inplace : bool.
            If False, this method returns a new Manatee DataFrame.
            If True, the current dataframe is mutated in-place, and this method returns nothing.
        """

        # If a string is passed, just drop that column
        if isinstance(columns, str):
            df = super(self.__class__, self).drop(columns)

        # If a tuple or list is passed, drop them all
        elif isinstance(columns, (list, tuple)):
            df = self
            for column in columns:
                df = df.drop(column)

        if inplace:
            self.__init__(df)
        else:
            return Manatee(df)


    def toPySparkDF(self):
        """
        Returns a PySpark DataFrame object.

        Returns
        -------
        df : pyspark.sql.dataframe.DataFrame.
        """
        return DataFrame(self._jdf, self.sql_ctx)


    # Function overloads

    def join(self, *args, **kwargs):
        return Manatee(super(self.__class__, self).join(*args, **kwargs))


    def select(self, *args, **kwargs):
        return Manatee(super(self.__class__, self).select(*args, **kwargs))


    def drop_duplicates(self, *args, **kwargs):
        return Manatee(super(self.__class__, self).drop_duplicates(*args, **kwargs))


    def agg(self, *args, **kwargs):
        return Manatee(super(self.__class__, self).agg(*args, **kwargs))


    def alias(self, *args, **kwargs):
        return Manatee(super(self.__class__, self).alias(*args, **kwargs))


    def filter(self, *args, **kwargs):
        return Manatee(super(self.__class__, self).filter(*args, **kwargs))


    # Dictionary for casting columns
    typedict = {
        int: IntegerType,
        bool: BooleanType,
        float: FloatType,
        date: DateType,
        str: StringType,
        datetime: TimestampType,
        None: NullType()
    }
