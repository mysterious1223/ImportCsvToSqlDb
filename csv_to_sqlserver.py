# author: kevin singh

try:
    import pandas as pd
    import pymssql as ms
    import sys
    import json
except Exception as e:
    print(str(e))

# interface for sql connectors
class SqlConnectorInterface:
    _db_obj = None

    def __init__(self, db_obj):
        self._db_obj = db_obj

    def connect_to_db(self) -> bool:
        pass

    def execute_sql_command(self, query) -> bool:
        pass

    def execute_many_query(self, query, data_list) -> bool:
        pass


class SqlServerConnector(SqlConnectorInterface):

    _conn = None
    _cursor = None

    def __init__(self, db_obj) -> None:
        super().__init__(db_obj)

    def connect_to_db(self) -> bool:
        # return connection
        print(self._db_obj._server)
        try:
            self._conn = ms.connect(
                self._db_obj._server,
                self._db_obj._username,
                self._db_obj._password,
                self._db_obj._database,
                100000,
            )
        except Exception as e:
            print(f"conn error: {str(e)}")
            print("Cant connect to db :(")
            return False
        finally:
            self._cursor = self._conn.cursor(as_dict=True)
            return True

    def execute_sql_command(self, query):
        try:
            self._cursor.execute(query)
            self._conn.commit()
            print("New Schema Created!")
            return True
        except:
            print(f"Execution error on command {query}")
            return False

    def execute_many_query(self, query, data_list):
        try:
            # print (query)
            # print (data_list)
            self._cursor.executemany(query, data_list)
            self._conn.commit()
            return True
        except Exception as e:
            print(f"Execution error on command: {str(e)}")
            return False


class database_connection_object:
    _server = ""
    _database = ""
    _username = ""
    _password = ""
    _conn_nm = ""
    _server_type = ""

    def __init__(self, server_type, conn_nm, server, database, username, password):
        self._server = server
        self._database = database
        self._username = username
        self._password = password
        self._conn_nm = conn_nm
        self._server_type = server_type


# load


def load_csv_into_memory(path):
    # load into conn (database)
    df = pd.read_csv(path)
    df.drop("Unnamed: 0", axis="columns", inplace=True)
    return df


def load_df_to_database_schema_table(data_df, schema, tbl_name, sql):

    # build a insert string

    insert_query_str = f"""
        insert into {schema}.{tbl_name} ("""

    for col in data_df.columns:
        if data_df.columns.get_loc(col) + 1 != len(data_df.columns):
            insert_query_str += col + ","
        else:
            insert_query_str += col + ") values ("

    for ind in range(len(data_df.columns)):
        if ind + 1 != len(data_df.columns):
            insert_query_str += "%s,"
        else:
            insert_query_str += "%s)"
    #print(insert_query_str)
    # create insert list...
    insert_data_list = []

    data_df = data_df.fillna(0)

    insert_data_list = [[x for x in ele] for ele in list(data_df.values)]

    res = sql.execute_many_query(insert_query_str, tuple(map(tuple, insert_data_list)))

    if res:
        print("Data imported to table")
        return True
    else:
        print("Data failed imported to table")
        return False


# helpers


# data checkers
def check_if_schema_exist_or_create(schema, sql):
    print("Checking Schema")
    query = f"""
        select s.name as schema_name, 
        s.schema_id,
        u.name as schema_owner
            from sys.schemas s
                inner join sys.sysusers u
                    on u.uid = s.principal_id
            where s.name = '{schema}'
            order by s.name
    """

    try:
        df = pd.read_sql(query, sql._conn)
        if len(df.index) > 0:
            print("Schema Exist!")
            return True
    except:
        print(f"{query} : failed!")
        return False

    query = f"""
        CREATE SCHEMA {schema}
    """

    return sql.execute_sql_command(query)


def check_if_table_exist_or_create(data_df, tbl_name, schema, sql):

    query = f"""
        
            select 
                t.name as table_name
            from sys.tables t
            where schema_name(t.schema_id) = '{schema}' and t.name = '{tbl_name}'
            order by table_name;
    """

    try:
        print("Checking Table")
        df = pd.read_sql(query, sql._conn)
        print(df)
        if len(df.index) > 0:
            print("table Exist!")
            # we need to drop
            res = sql.execute_sql_command(f"drop table {schema}.{tbl_name}")
            if res:
                print("table removed")
            else:
                print("table failed to remove ...")
                return False
            # return True
    except:
        print(f"{query} : failed!")
        return False

    create_query_str = f"""
        create table {schema}.{tbl_name} ("""

    # we need to build out a create table string
    # need to loop through the colunns
    try:
        data_df.columns = [x.replace(" ", "_") for x in data_df.columns]
        for col in data_df.columns:
            if data_df.columns.get_loc(col) + 1 != len(data_df.columns):
                create_query_str += f"""
                    {col} VARCHAR(MAX),"""
            else:
                create_query_str += f"""
                    {col} VARCHAR(MAX)
                )
                """
    except:
        print("Failed to build create table string...")
        return False

    print(create_query_str)
    res = sql.execute_sql_command(create_query_str)

    if res:
        print("New table Created!")
        return True
    else:
        print("New table creation failed")
        return False


def parse_json_config_todbobj(config_loc):
    f = open(
        config_loc,
    )
    data = json.load(f)
    data = data["dbconfig"][0]
    return database_connection_object(
        data["server_type"],
        data["conn_name"],
        data["server"],
        data["database"],
        data["username"],
        data["password"],
    )


# this will need to be loaded through a config
# load in a CSV? with config info?


def __main__():
    print("Starting loader")
    if len(sys.argv) != 4:
        print("Incorrect args")
        print("app <csv> <schema> <json config>")
        quit()
    csv_file = sys.argv[1]
    schema_name = sys.argv[2]
    db_json_config = sys.argv[3]
    db_con_obj = parse_json_config_todbobj(db_json_config)
    print(f"Loading {csv_file}")
    sql = SqlServerConnector(db_con_obj)
    sql.connect_to_db()
    csv_df = load_csv_into_memory(csv_file)
    csv_name = csv_file.split(".")[0]

    if not check_if_schema_exist_or_create(schema_name, sql):
        print("Failed to create schema")
        quit()
    if not check_if_table_exist_or_create(csv_df, csv_name, schema_name, sql):
        print("Failed to create table")
        quit()
    if not load_df_to_database_schema_table(csv_df, schema_name, csv_name, sql):
        print("Failed to import data")
        quit()

    print("Loader complete!")


__main__()
