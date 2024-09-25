import pandas as pd
import pymysql
import concurrent.futures


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
                    self.port = kwargs['port']
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
        if self.db_type == 'mysql':
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

class Loader:
    """Load a table with multithreading"""
    def __init__(self, table: str, connector: Connector, fields: str=None, chunk_size: int=1000, query: str=None):
        # Config the table information
        self.fields = ['all' if fields is None else fields] # Query all the fields if user do NOT indicate.
        self.table = table
        self.conn = connector.conn
        self.cursor = connector.cursor

        self.cursor.execute(f"""SELECT COUNT(*) FROM {table}""")
        self.table_size = self.cursor.fetchone()[0]

        self.chunk_size = chunk_size
        self.num_chunks = self.table_size // self.chunk_size + (1 if self.table_size % self.chunk_size > 0 else 0)
        self.query = query # user's query

    def load_large_data(self):
        all_data = pd.DataFrame()  # Store all the data from SQL

        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = [] # tasks queue
            # Commit multithreading tasks
            for i in range(self.num_chunks):
                offset = i * self.chunk_size
                futures.append(executor.submit(self.read_chunked_data, offset))

            # Collect the result after all the tasks are completed.
            for future in concurrent.futures.as_completed(futures):
                data_chunk = future.result()
                all_data = pd.concat([all_data, data_chunk], ignore_index=True)  # Merge tasks' data.

        return all_data

    def read_chunked_data(self, offset):
        # Execute embedded query either user's modified query.
        if self.query is None:
            if self.fields == 'all':
                query = f"""SELECT * FROM {self.table} LIMIT {self.chunk_size} OFFSET {offset}"""
            else:
                fields_string = ','.join(self.fields)
                query = f"""SELECT {fields_string} FROM {self.table} LIMIT {self.chunk_size} OFFSET {offset}"""
        else: # In case of 'WHERE' etc. is added by user.
            query = f"""{self.query} LIMIT {self.chunk_size} OFFSET {offset}"""

        return pd.read_sql(query, self.conn)

    def load_json(self, table: str):
        pass

    def parse_json(self):
        pass

