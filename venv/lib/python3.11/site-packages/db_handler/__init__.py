import pyodbc
""" Run .help() to get the link to the documentation """

class ACCESSdbc:
    """ Connect to a Microsoft Access database
     :param db_path: The path of the database file
    :return: None
        """
    def __init__(self, db_path):
        self.db_path = db_path
        self.connection = None

    def connect(self):
        """ Connect to a Microsoft Access database """
        try:
            self.connection = pyodbc.connect(r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + self.db_path)
            print("Connected to the access database.")
            return True
        except pyodbc.Error as e:
            print(f"Error connecting to SSMS database: {str(e)}")
            return False

    def disconnect(self):
        """ Disconnect from a Microsoft Access database """
        if self.connection:
            self.connection.close()
            print("Disconnected from the database.")
    
    def execute_query_return_results(self, query):
        """ Execute a database query and return the results
         :param query: The query to execute
         :return: The results of the query """
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            self.connection.commit()
            if results == None:
                return []
            return results
        except pyodbc.Error as e:
            print(f"The error '{str(e)}' occurred")

            



class SSMSdbc:
    """ Connect to a Microsoft SQL Server database
     :param server: The server name
     :param database: The database name
     :param username: The username
     :param password: The password
     :return: None """
    def __init__(self, server, database, username, password):
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.connection = None

    def connect(self):
        """ Connect to a Microsoft SQL Server database
          """
        try:
            connection_string = f"DRIVER={{SQL Server}};SERVER={self.server};DATABASE={self.database};UID={self.username};PWD={self.password}"
            self.connection = pyodbc.connect(connection_string)
            print("Connected to SSMS database successfully!")
            return True
        except pyodbc.Error as e:
            print(f"Error connecting to SSMS database: {str(e)}")
            return False

    def disconnect(self):
        if self.connection:
            self.connection.close()
            print("Disconnected from SSMS database.")
    #make a function to execute a query and return the results
    def execute_query_return_results(self, query):
        """ 
         Execute a database query and return the results
         :param query: The query to execute
         :return: The results of the query
           """
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            self.connection.commit()
            return results
        except pyodbc.Error as e:
            print(f"The error '{str(e)}' occurred")

def help():
    print('https://github.com/LucSIGIER-LIAL/db_handler/blob/main/README.md')