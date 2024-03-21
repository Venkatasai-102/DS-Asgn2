import sqlite3
import os
from fastapi import FastAPI
import mysql.connector as conn
import time
import inspect
import threading
DB_FILE = "example.db"
if os.path.exists(DB_FILE):
    os.remove(DB_FILE)

# mysql_conn = sqlite3.connect('example.db', check_same_thread=False)
# mysql_cursor = mysql_conn.cursor()

# while True:
#     try:
#         mysql_conn = conn.connect(
#             host="metadb",
#             user=os.getenv("MYSQL_USER", "bhanu"),
#             password=os.getenv("MYSQL_PASSWORD", "bhanu@1489"),
#             database=os.getenv("MYSQL_DATABASE", "StudentDB"),
#         )
#         print("connected")
#         break
    
#     except Exception as e:
#         # print(e)
#         time.sleep(0.02)
# mysql_cursor = mysql_conn.cursor()

app = FastAPI()

MAX_REQUEST_COUNT = 1e6
MAX_SERVER_INDEX = 1024
NUM_SLOTS = 512
VIR_SERVERS = 9


# maps for storing locally for loadbalancer
app.locks = {}
app.hash_dict = {}
app.server_list = {}
app.schema = None


# locks and semaphores for handling concurrency

app.read_write_lock_dict = {}
app.read_semaphore_dict = {}

READ_LOCK = threading.Lock()
WRITE_LOCK = threading.Lock()
READER_COUNT = 0

def acquire_read(): 
    READ_LOCK.acquire()
    global READER_COUNT
    READER_COUNT += 1
    if READER_COUNT == 1:
        WRITE_LOCK.acquire()
    READ_LOCK.release()
    caller_name = inspect.stack()[1].function
    print(f"acquired read lock, called by {caller_name}")

def release_read():
    READ_LOCK.acquire()
    global READER_COUNT
    READER_COUNT -= 1
    if READER_COUNT == 0:
        WRITE_LOCK.release()
    READ_LOCK.release()
    caller_name = inspect.stack()[1].function
    print(f"released read lock, called by {caller_name}")

def acquire_write():
    WRITE_LOCK.acquire()
    caller_name = inspect.stack()[1].function
    print(f"acquired write lock, called by {caller_name}")

def release_write():
    WRITE_LOCK.release()
    caller_name = inspect.stack()[1].function
    print(f"released write lock, called by {caller_name}")

def get_db(db_engine = "sqlite"):
    if db_engine == "sqlite":
        db_conn = sqlite3.connect('example.db')
        db_cursor = db_conn.cursor()
    else:
        db_conn= conn.connect(
                host="metadb",
                user=os.getenv("MYSQL_USER"),
                password=os.getenv("MYSQL_PASSWORD"),
                database=os.getenv("MYSQL_DATABASE"),
            )
        db_cursor = db_conn.cursor()
    return db_conn, db_cursor

def close_db(db_conn, db_cursor):
    db_cursor.close()
    db_conn.close()

