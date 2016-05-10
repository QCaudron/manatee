def silence(context=None):
    """
    Removes INFO and WARN messages from the specified context, leaving only ERROR messages.
    If no context is passed, this function looks through globals() to find any SparkContexts,
    SQLContexts, and HiveContexts, and silences those.
    """

    if context:
        logger = context._jvm.org.apache.log4j
        logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
        logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

    else:
        to_be_silenced = [var for var in globals().values()
                          if (isinstance(var, pyspark.sql.context.HiveContext)
                              or isinstance(var, pyspark.sql.context.SQLContext)
                              or isinstance(var, pyspark.context.SparkContext))]

        for context in to_be_silenced:
            silence(context)
