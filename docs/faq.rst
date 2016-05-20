Frequently Asked Questions
==========================

Why isn't the main *manatee* object called a DataFrame ?
  Mostly because you may want to have ``DataFrame`` imported from PySpark. Also because using objects named after large marine mammals is fun.

Why are some functions overloaded whilst others are new ?
  We try to ensure that any added functionality doesn't override what you're used to in PySpark DataFrames. Therefore, methods offering new
  functionality have new names. Names are chosen to feel something like *pandas* DataFrames. Any method with the same name as something
  that already exists in PySpark DataFrames are just redefined to return Manatee objects instead of PySpark DataFrames.

What's the difference between a *dataframe* and a *DataFrame* ?
  The *dataframe* is a fantastic data structure familiar to users of *pandas* or *R*. Both of these languages implement a dataframe object,
  ``pandas.DataFrame`` and ``data.frame`` respectively. PySpark implements one too : ``pyspark.sql.dataframe.DataFrame``. Manatee inherits from
  this object, and so provides all of ``pyspark.sql.dataframe.DataFrame``'s functionality, and more, as the object ``manatee.manatee.Manatee``.
  In the docs, this is referred to as a *Manatee DataFrame*. Whenever *DataFrame* is used, it refers to a specific object or implementation. When
  you see *dataframe*, it's just talking about the generic data structure.

Are these questions really frequently asked ?
  No, I just wanted somewhere logical to explain a few things. If you have any questions, don't hesitate to open a Github issue !
