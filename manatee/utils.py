def silence(context):
    """
    Silences the PySpark log messages.

    Removes INFO and WARN messages from the specified context, leaving only ERROR messages.
    """

    logger = context._jvm.org.apache.log4j
    logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)
    logger.LogManager.getRootLogger().setLevel(logger.Level.ERROR)
