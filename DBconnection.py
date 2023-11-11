from mysql.connector import connection
import pandas as pd
import os

class DBconn:

    def __init__(self) -> None:
        PATH_TO_CRED = "./.."

        url, BD, user, passw = open(os.path.join(PATH_TO_CRED, "credenciales.txt"), 'r').readlines()

        self.conn = connection.MySQLConnection(host=url, database=BD, user=user, password=passw)


    def query(self, sql):
    
        if not self.conn.is_connected():
            self.conn.reconnect(attempts=2, delay=0.5)

        with self.conn.cursor() as cursor:
            cursor.execute(sql)

            res = pd.DataFrame(
                data = cursor.fetchall(),
                columns = cursor.column_names
            )

        return res.squeeze()

    
    def close(self):
        self.conn.close()