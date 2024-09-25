import pymysql


def setting(path):
    """
    Parsing the setting of a certain database.
    :param path:
    :return:
    """
    pass

class Connector:
    """
    Database Connector
    """
    def __init__(self, *args, **kwargs):
        """
        To set the database connection configuration.
        :param args: Location 1: the type of database; Location 2: the setting file of database, if it's missing, please infer the connection setting in kwargs.
        :param kwargs: You should input the setting information if there is no setting file in args.
        """
        # Parsing the connection configuration
        try:
            self.db_type = args[0] # The type of database.
            try: # Parsing connection settings from the setting file.
                self.db_setting_file = args[1]
                pass # TODO: Add parsing algorithm.
            except IndexError as e: # Parsing connection settings from kwargs.
                try:
                    self.db_setting = kwargs
                    self.host = kwargs['host']
                    self.user = kwargs['user']
                    self.key = kwargs['key']
                    self.db = kwargs['db']
                except KeyError as e:
                    print(f"Missing parameter: {e}")
        except IndexError as e:
            print(f"Insufficient setting parameters!")

        # Obtain the connection and cursor
        self.conn, self.cursor = self.connect()

    def connect(self):
        """
        Obtain the connection.
        :return:
            1. self.conn: the connection
            2. self.cursor: the cursor
        """
        if self.db == 'mysql':
            conn  = pymysql.connect(
                user=self.user,
                password=self.key,
                host=self.host,
                database=self.db
            )
            cursor = conn.cursor()

        else:
            pass # TODO: Add other database connection.
        return conn, cursor