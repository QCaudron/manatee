from pyspark.sql.context import SQLContext, HiveContext
from pyspark.context import SparkContext

def silence(context=None):
    """
    Silences the PySpark log messages.

    Removes INFO and WARN messages from the specified context, leaving only ERROR messages.
    If no context is passed, this function looks through globals() to find any SparkContexts,
    SQLContexts, and HiveContexts, and silences those.
    """

    if context:
        logger = context._jvm.org.apache.log4j
        logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
        logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)
        logger.LogManager.getRootLogger().setLevel(logger.Level.ERROR)
        print("Silenced {}.".format(context))

    else:
        to_be_silenced = [var for var in globals().values()
                          if (isinstance(var, HiveContext)
                           or isinstance(var, SQLContext)
                           or isinstance(var, SparkContext))]

        for context in to_be_silenced:
            silence(context)
