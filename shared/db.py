import duckdb

con = duckdb.connect()

def get_connection():
    """
    Returns a connection to the database.
    :return: duckdb connection
    """
    return con

def executeCustomQueryDF(query):
    """
    This executes a query and returns the dataframe.
    :param query: The query to execute
    :return: query result as dataframe
    """
    return con.execute(query).fetch_df()