from random import randint
import uvicorn
import os
import time
from consistent_hashing import ConsistentHashing
import requests
from requests import RequestException
from docker import errors
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse,RedirectResponse
import mysql.connector as conn
from helpers import create_server, remove_servers, get_servers
import sqlite3

print("Starting Load Balancer......")

# while True:
#     try:
#         mysql_conn = conn.connect(
#             host="metadb",
#             user=os.getenv("MYSQL_USER", "user"),
#             password=os.getenv("MYSQL_PASSWORD", "Pass@123"),
#             database=os.getenv("MYSQL_DATABASE", "StudentDB"),
#         )
#         print("connected")
#         break
    
#     except Exception as e:
#         print(e)
#         time.sleep(0.02)

# mysql_cursor = mysql_conn.cursor()
# print("Connected to MySQL!")

import os
db_file = "example.db"
if os.path.exists(db_file):
    os.remove(db_file)

mysql_conn = sqlite3.connect('example.db')
mysql_cursor = mysql_conn.cursor()


app = FastAPI()

MAX_REQUEST_COUNT = 1e6
MAX_SERVER_INDEX = 1024
NUM_SLOTS = 512
VIR_SERVERS = 9


# maps for storing locally for loadbalancer
app.hash_dict = {}
app.server_list = {}

# locks and semaphores for handling concurrency
app.read_write_lock_dict = {}
app.read_semaphore_dict = {}


@app.post("/init")
async def init_system(request: Request):
    req = await request.json()
    n, schema = req['N'], req['schema']
    shards, servers = req['shards'], req['servers']
    print(servers)

    if n != len(shards) or n != len(servers):
        return JSONResponse(
            status_code=400,
            content={
                "message": f"value of N and number of shards/servers don't match!",
                "status": "failure"
            }
        )
        
    # To store the shards map with server in database
    # MapT Schema
    create_mapt_query = f"""
    CREATE TABLE IF NOT EXISTS MapT (
        Shard_id VARCHAR(255),
        Server_id VARCHAR(255)
    )
    """
    mysql_cursor.execute(create_mapt_query)
    mysql_conn.commit()

    # To create ShardT schema in database
    create_shardt_query = f"""
    CREATE TABLE IF NOT EXISTS ShardT (
        Stud_id_low INT PRIMARY KEY,
        Shard_id VARCHAR(255),
        Shard_size INT,
        valid_idx INT
    )
    """
    mysql_cursor.execute(create_shardt_query)
    mysql_conn.commit()
    
    ip={}
    for server_name in servers:
        name,ipaddr = create_server(name=server_name)
        ip[name] = ipaddr
    
    for server_name in servers:
        url = f"http://{ip[server_name]}:{8000}/config"
        print(url)
        data = {
            "schema": schema,
            "shards": [sh["Shard_id"] for sh in shards]
        }
        print(data)
        # time.sleep(10)
        while True:
           try:
                result = requests.post(url, json=data,timeout=None)
                print(result.ok)
                break
           except requests.RequestException as e:
                print("trying again")
                time.sleep(2) # time sleep for sqlite is 2 sec and for mysql need change to 30 sec
            
        # on success
        app.server_list[server_name] = {"index": randint(1, MAX_SERVER_INDEX), "ip": ip[server_name]}
        for sh in servers[server_name]:
            if sh not in app.hash_dict:
                app.hash_dict[sh] = ConsistentHashing(NUM_SLOTS, VIR_SERVERS)
            
            app.hash_dict[sh].add_server(app.server_list[server_name]['index'], ip[server_name], 8000)
            
            ## add shard-server mapping to database
            add_mapt_query = "INSERT INTO MapT VALUES (?, ?)"
            print(add_mapt_query)
            try:
                mysql_cursor.execute(add_mapt_query,(sh,server_name))
                mysql_conn.commit()
            except Exception as e:
                print(e)
                print("Issue is here")

    # creating all shard entries in ShardT
    for shard in shards:
        shard_query = "INSERT INTO ShardT VALUES (?,?,?,?)"

        try:
            mysql_cursor.execute(shard_query,(shard["Stud_id_low"],shard["Shard_id"],shard["Shard_size"],shard["Stud_id_low"]))
            mysql_conn.commit()
        except Exception as e:
                print(e)
                print("Issue is here ):")
            
    return {
        "message" : "Configured Database",
        "status" : "success"
    }

@app.get("/status")
async def get_status():
    try:
        get_shards_query = "SELECT Stud_id_low, Shard_id, Shard_size FROM ShardT"
        mysql_cursor.execute(get_shards_query)
        shard_rows = mysql_cursor.fetchall()
        
        servers = {}
        for ser in app.server_list:
            get_servers = "SELECT Shard_id FROM MapT WHERE Server_id=?"
            mysql_cursor.execute(get_servers, (ser, ))
            sh_rows = mysql_cursor.fetchall()
            servers[ser] = [sh[0] for sh in sh_rows]
        
        return {
            "N": len(app.server_list), # number of servers currently running
            "schema": {
                "columns":["Stud_id","Stud_name","Stud_marks"],
                "dtypes":["Number","String","String"]
            },
            "shards": [
                {col: value for col, value in zip(["Stud_id_low", "Shard_id", "Shard_size"], row)}
                for row in shard_rows
            ],
            "servers": servers
        }
    except Exception as e:
        print("Exception:", e)
        return JSONResponse(status_code=500, content={"message": "Internal Server Error"})
        
@app.post("/add")
async def add_servers(request: Request):
    req = await request.json()
    n = req['n']
    new_shards, servers = req['new_shards'], req['servers']

    if n > len(servers):
        return JSONResponse(
            status_code=400,
            content={
                "message": "<Error> Number of new servers (n) is greater than newly added instances",
                "status": "failure"
            }
        )
    
    # need to create random server id when not given, currently assuming n = len(servers)
    ip = {}
    for server_name in servers:
        name , ipaddr = create_server(name=server_name)
        print("Successfully created ",name)
        ip[name] = ipaddr
    
    message = "Added"
    
    for server_name in servers:
        # on success, adding this new server to the respective shards' consistent hashing object
        app.server_list[server_name] = {"index": randint(1, MAX_SERVER_INDEX), "ip": ip[server_name]}
        for sh in servers[server_name]:
            message += f" {server_name},"
            if sh not in app.hash_dict:
                app.hash_dict[sh] = ConsistentHashing(NUM_SLOTS, VIR_SERVERS)
            
            app.hash_dict[sh].add_server(app.server_list[server_name]['index'], ip[server_name], 8000)

            # need to handle the case when one of the server fails, we need to stop the already created servers or do something else
            # adding entry in MapT

            add_mapt_query = "INSERT INTO MapT VALUES (?,?)"
            mysql_cursor.execute(add_mapt_query,(sh,server_name))
            mysql_conn.commit()
        
        # calling the /config api of this new server
        url = f"http://{ip[server_name]}:{8000}/config"
        data = {
            "schema": {"columns":["Stud_id","Stud_name","Stud_marks"],
                       "dtypes":["Number","String","String"]},
            "shards": servers[server_name]
        }
        print(data)
        while True:
            try:
                requests.post(url, json=data)
                break
            except:
                print("retry after sleeping 30sec")
                time.sleep(30)
        
    # adding entries in ShardT
    for shard in new_shards:
        shard_query ="INSERT INTO ShardT VALUES (?,?,?,?)"
        mysql_cursor.execute(shard_query,(shard["Stud_id_low"],shard["Shard_id"],shard["Shard_size"],shard["Stud_id_low"]))
        mysql_conn.commit()


    return {
        "N": len(app.server_list),
        "message": message,
        "status": "success"
    }

@app.post("/read")
async def read_data(request: Request):
    req = await request.json()
    stud_low = req["Stud_id"]["low"]
    stud_high = req["Stud_id"]["high"]

    # need to optimize to selectively get the shard entries instead of getting all shards in ShardT
    get_shards_query = "SELECT * from ShardT"
    mysql_cursor.execute(get_shards_query)
    response = mysql_cursor.fetchall()
    request_id = randint(1, MAX_REQUEST_COUNT)%NUM_SLOTS

    shards_queried = []

    valid_rows = []
    for row in response:
        if not (stud_low >= row[3] or row[0] > stud_high):
            valid_rows.append(row)
            shards_queried.append(row[1])

    result = []

    for row in valid_rows:
        consistent_hashing_object = app.hash_dict[row[1]]
        server = consistent_hashing_object.get_nearest_server(request_id)

        url = f"http://{server.server_ip}:{8000}/read"
        data = {
            "shard": row[1],
            "Stud_id": {
                "low": max(row[0], stud_low),
                "high": min(row[0]+row[3], stud_high)
            }
        }

        resp = requests.post(url, json=data)
        
        if resp.status_code != 200:
            return JSONResponse(
                status_code=400,
                content={
                    "message": "Invalid status code",
                    "status": "failure"
                }
            )
        
        resp = resp.json()

        if resp["status"] == "success":
            result += resp["data"]
        else:
            return JSONResponse(
            status_code=400,
            content={
                "message": "Invalid query",
                "status": "failure"
            }
        )
    
    return {
        "shards_queried": shards_queried,
        "data": result,
        "status": "success"
    }


@app.delete("/rm")
async def rm_servers(request: Request):
    try:
        req = await request.json()
        n = req["n"]
        servers = req["servers"]

        if n < len(servers):
            return JSONResponse(
                status_code=400,
                content={
                    "message": "<Error> Length of server list is more than removable instances",
                    "status" : "failure"
                }
            )
        
        if n > len(app.server_list):
            return JSONResponse(
                status_code=400,
                content={
                    "message": "<Error> Length of server list is more than number of servers present",
                    "status" : "failure"
                }
            )

        if n > len(servers):
            for _ in range(n-len(servers)):
                for ser in app.server_list:
                    if ser not in servers:
                        servers.append(ser)
                        break
        
        removing_servers = {}
        rm_ser_list = []
        for ser in servers:
            removing_servers[ser] = app.server_list[ser]
            app.server_list.pop(ser)
            rm_ser_list.append(ser)
        
        remove_servers(rm_ser_list)
        
        for ser in removing_servers:
            find_shards_query = "SELECT Shard_id FROM MapT WHERE Server_id=?"
            mysql_cursor.execute(find_shards_query, (ser, ))
            shards = mysql_cursor.fetchall()

            remove_entry_query = "DELETE FROM MapT WHERE Server_id=?"
            mysql_cursor.execute(remove_entry_query, (ser, ))

            mysql_conn.commit()

            for sh in shards:
                app.hash_dict[sh[0]].remove_server(removing_servers[ser]["index"])

                check_shard_query = "SELECT * FROM MapT WHERE Shard_id=?"
                mysql_cursor.execute(check_shard_query, (sh[0], ))
                chk = mysql_cursor.fetchall()

                if len(chk) == 0:
                    app.hash_dict.pop(sh[0])
                    remove_shard_query = "DELETE FROM ShardT WHERE Shard_id=?"
                    mysql_cursor.execute(remove_shard_query, (sh[0], ))

                    mysql_conn.commit()
                
        return {
            "message": {
                "N": len(app.server_list),
                "servers": rm_ser_list
            },
            "status" : "successful"
        }
    except errors.DockerException as e:
        print(e)
        raise HTTPException(status_code=500, detail="Docker removal error")

    except RequestException as e:
        print("Request Exception:", e)
        return JSONResponse(status_code=500, content={"message":"Request Failure", "status": "failure"})
    
    except sqlite3.Error as e:
        print("Exeption:", e)
        return JSONResponse(status_code=500, content={"message": "Sqlite3 error", "status": "failure"})

    except Exception as e:
        print("Exception:", e)
        return JSONResponse(status_code=500, content={"message": "Unexpected error", "status": "failure"})

    
    
# Run the FastAPI app
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)