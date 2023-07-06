from dotenv import load_dotenv
from unhandle import GlobalException
import pymysql
import os
load_dotenv()


class Database:
    def __init__(self):
        self.cur = None
        self.con = None
        self.host = os.environ.get('HOST')
        self.user = os.environ.get('DB_USER')
        self.password = os.environ.get('PASSWORD')
        self.db = os.environ.get('DB')
        self.charset = os.environ.get('CHARSET')

    def connect_db(self):
        try:
            self.con = pymysql.connect(host=self.host, user=self.user, password=self.password, db=self.db, charset=self.charset)
            self.cur = self.con.cursor()
        except Exception as e:
            raise GlobalException(f"Error: Database connection failed: {e}")

    def disconnect_db(self):
        try:
            if self.cur:
                self.cur.close()
            if self.con and self.con.open:
                self.con.close()
        except Exception as e:
            raise GlobalException(f"Error: Database disconnection failed {e}")


