import sqlite3
import os
from fastapi import FastAPI

db_file = "example.db"
if os.path.exists(db_file):
    os.remove(db_file)

mysql_conn = sqlite3.connect('example.db', check_same_thread=False)
mysql_cursor = mysql_conn.cursor()


app = FastAPI()

MAX_REQUEST_COUNT = 1e6
MAX_SERVER_INDEX = 1024
NUM_SLOTS = 512
VIR_SERVERS = 9


# maps for storing locally for loadbalancer
app.hash_dict = {}
app.server_list = {}
app.schema = None

# locks and semaphores for handling concurrency
app.read_write_lock_dict = {}
app.read_semaphore_dict = {}