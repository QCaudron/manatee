def silence(context, warn=False):
    """
    Silences the PySpark log messages.

    Removes INFO ( and optionally WARN ) messages from the specified context's logger.

    Parameters
    ----------
    context : SparkContext, SQLContext, or HiveContext
        For example, if `sc` is your SparkContext, calling ``silence(sc)`` will switch
        off INFO and WARN messages issued from this SparkContext.
    warn : bool.
        If True, the logger's level is set to WARN rather than ERROR, so the context
        will continue to issue WARN messages as well as ERROR messages.
        If False, only ERROR messages are issued.
    """

    logger = context._jvm.org.apache.log4j
    target_level = logger.Level.WARN if warn else logger.Level.ERROR

    logger.LogManager.getLogger("org"). setLevel(target_level)
    logger.LogManager.getLogger("akka").setLevel(target_level)
    logger.LogManager.getRootLogger().setLevel(target_level)
