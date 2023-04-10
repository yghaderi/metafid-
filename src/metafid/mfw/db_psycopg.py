import pandas as pd
import sqlalchemy
import psycopg


class DB:

    def __init__(self, dbname: str, user: str, pass_: str, ):
        """
        Initial DB.
        :param dbname: Database name.
        :param user: Database user.
        :param pass_: Database password.
        """
        self.dbname = dbname
        self.user = user
        self.pass_ = pass_
        self.engine = sqlalchemy.create_engine(f"postgresql://{self.user}:{self.pass_}@localhost:5432/{self.dbname}")

    def insert_data(self, table: str, df: pd.DataFrame):
        """
        Insert tata to DB. Note that the names of the columns in DF must be the same as the names of the columns in table.
        :param table: Table name.
        :param df: Pandas DataFrame¬
        :return: push data to db
        """
        records = list(df.itertuples(index=False, name=None))
        cols = f"{tuple(i for i in df.columns)}".replace("'", "").replace('"', "")
        with psycopg.connect(f"dbname={self.dbname} user={self.user} password={self.pass_}") as conn:
            # Open a cursor to perform database operations
            with conn.cursor() as cur:
                with cur.copy(f"COPY {table} {cols} FROM STDIN") as copy:
                    for record in records:
                        copy.write_row(record)
                # Make the changes to the database persistent
                conn.commit()

    def drop_all(self, table: str):
        """
        Drop all records from table.
        :param table: Table name.
        :return: Drop all records.
        """
        with psycopg.connect(f"dbname={self.dbname} user={self.user} password={self.pass_}") as conn:
            # Open a cursor to perform database operations
            with conn.cursor() as cur:
                cur.execute(f"DELETE FROM {table}")
                conn.commit()

    def _query(self, table: str, cols: str):
        """
        Query for take all data.
        :param table: Table name.
        :param cols: String tuple of columns (ex. "(col1, col2, col3)")
        :return: Pandas DataFrame of the all data of selected columns from table
        """
        columns = cols.replace(" ", "").replace("(", "").replace(")", "").split(",")
        with psycopg.connect(f"dbname={self.dbname} user={self.user} password={self.pass_}") as conn:
            # Open a cursor to perform database operations
            with conn.cursor() as cur:
                cur.execute(f"SELECT {cols} FROM {table}")
                cur.fetchone()
                conn.commit()
                return pd.DataFrame([i[0] for i in cur], columns=columns)
    
    

    def query_all(self, table: str, cols: str):
        """
        Query for take all data.
        :param table: Table name.
        :param cols: String tuple of columns (ex. "col1,col2,col3" or "*")
        :return: Pandas DataFrame of the all data of selected columns from table
        """
        return pd.read_sql_query(f"""select {cols} from {table}""", con=self.engine)
    
    def query_where(self, table: str, cols: str, where:str):
        """
        Query for take certain conditions apply.
        :param table: Table name.
        :param cols: String tuple of columns (ex. "col1,col2,col3" or "*")
        :return: Pandas DataFrame of the all data of selected columns from table
        """
        return pd.read_sql_query(f"""select {cols} from {table} where {where}""", con=self.engine)
