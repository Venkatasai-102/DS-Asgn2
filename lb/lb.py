from random import randint
import uvicorn
import subprocess
import os
import time
from consistent_hashing import ConsistentHashing
import requests
import asyncio
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse,RedirectResponse
import mysql.connector as conn
from helpers import create_server, remove_servers, get_servers

print("Strating Load Balancer......")

while True:
    try:
        mysql_conn = conn.connect(
            host="metadb",
            user=os.getenv("MYSQL_USER", "bhanu"),
            password=os.getenv("MYSQL_PASSWORD", "bhanu@1489"),
            database=os.getenv("MYSQL_DATABASE", "StudentDB"),
        )
        print("connected")
        break
    
    except Exception as e:
        print(e)
        time.sleep(0.02)

mysql_cursor = mysql_conn.cursor()
print("Connected to MySQL!")


app = FastAPI()

MAX_REQUEST_COUNT = 1e6
MAX_SERVER_INDEX = 1024
NUM_SLOTS = 512
VIR_SERVERS = 9


# maps for storing locally for loadbalancer
app.hash_dict = {}
app.server_list = {}
app.read_write_lock_dict = {}
app.read_semaphore_dict = {}


@app.post("/init")
async def init_system(request: Request):
    req = await request.json()
    n, schema = req['N'], req['schema']
    shards, servers = req['shards'], req['servers']
    
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
    
    for server_name in servers:
        result = create_server(server_name)
        if result['status'] == 'failure':
            return result['response']

        # need to change to the network name currently put as localhost for testing
        url = f"http://{result['ipaddr']}:{8000}/config"
        print(url)
        data = {
            "schema": schema,
            "shards": [sh["Shard_id"] for sh in shards]
        }
        # time.sleep(10)
        while True:
           try:
                requests.post(url, data,timeout=None)
                break
           except Exception as e:
                # print("trying again")
               pass
            
        # on success
        app.server_list[server_name] = {"index": randint(1, MAX_SERVER_INDEX), "ip": result['ipaddr']}
        for sh in servers[server_name]:
            if sh not in app.hash_dict:
                app.hash_dict[sh] = ConsistentHashing(NUM_SLOTS, VIR_SERVERS)
                app.read_write_lock_dict[sh] = asyncio.Lock()
                app.read_semaphore_dict[sh] = asyncio.Semaphore(2)
            
            app.hash_dict[sh].add_server(app.server_list[server_name]['index'], result['ipaddr'], 8000)
            
            # need to handle the case when one of the server fails, we need to stop the already created servers or do something else
            ## add shard-server mapping to database
            add_mapt_query = f"""
            INSERT INTO MapT VALUES ({sh}, {server_name})
            """
            mysql_cursor.execute(add_mapt_query)
            mysql_conn.commit()

    # creating all shard entries in ShardT
    for shard in shards:
        shard_query = f'INSERT INTO ShardT VALUES ({shard["Stud_id_low"]}, {shard["Shard_id"]}, {shard["Shard_size"]}, {shard["Stud_id_low"]})'
        mysql_cursor.execute(shard_query)
        mysql_conn.commit()

  
@app.post("/add")
async def add_servers(request: Request):
    req = await request.json()
    n = req['n']
    new_shards, servers = req['shards'], req['servers']

    if n > len(servers):
        return JSONResponse(
            status_code=400,
            content={
                "message": "<Error> Number of new servers (n) is greater than newly added instances",
                "status": "failure"
            }
        )
    
    # need to create random server id when not given, currently assuming n = len(servers)

    for server_name in servers:
        result = create_server(server_name)
        
        if result['status'] == 'failure':
            return result['response']
            
        # on success, adding this new server to the respective shards' consistent hashing object
        app.server_list[server_name] = {"index": randint(1, MAX_SERVER_INDEX), "ip": result['ipaddr']}
        for sh in servers[server_name]:
            if sh not in app.hash_dict:
                app.hash_dict[sh] = ConsistentHashing(NUM_SLOTS, VIR_SERVERS)
                app.read_write_lock_dict[sh] = asyncio.Lock()
                app.read_semaphore_dict[sh] = asyncio.Semaphore(2)
            
            app.hash_dict[sh].add_server(app.server_list[server_name]['index'], result['ipaddr'], 8000)

            # need to handle the case when one of the server fails, we need to stop the already created servers or do something else
            # adding entry in MapT
            add_mapt_query = f"""
            INSERT INTO MapT VALUES ({sh}, {server_name})
            """
            mysql_cursor.execute(add_mapt_query)
            mysql_conn.commit()
        
        # calling the /config api of this new server
        url = f"http://{result['ipaddr']}:{8000}/config"
        data = {
            "schema": {"columns":["Stud_id","Stud_name","Stud_marks"],
                       "dtypes":["Number","String","String"]},
            "shards": [sh["Shard_id"] for sh in servers[server_name]]
        }

        requests.post(url, data)
    
    # adding entries in ShardT
    for shard in new_shards:
        shard_query = f'INSERT INTO ShardT VALUES ({shard["Stud_id_low"]}, {shard["Shard_id"]}, {shard["Shard_size"]}, {shard["Stud_id_low"]})'
        mysql_cursor.execute(shard_query)
        mysql_conn.commit()

@app.get("/read")
async def read_data(request: Request):
    req = await request.json()
    stud_low = req["Stud_id"]["low"]
    stud_high = req["Stud_id"]["high"]

    # need to optimize to selectively get the shard entries instead of getting all shards in ShardT
    get_shards_query = "SELECT * from ShardT"
    mysql_cursor.execute(get_shards_query)
    response = mysql_cursor.fetchall()
    request_id = randint()%NUM_SLOTS

    shards_queried = []

    valid_rows = []
    for row in response:
        if not (stud_low > row[0]+row[3] or row[0] > stud_high):
            valid_rows.append(row)
            shards_queried.append(row[1])

    result = []

    for row in valid_rows:
        consistent_hashing_object = app.hash_dict[row[1]]
        server = consistent_hashing_object.get_nearest_server(request_id)

        url = f"{server.server_ip}:{8000}/read"
        data = {
            "low": max(row[0], stud_low),
            "high": min(row[0]+row[3], stud_high)
        }

        resp = request.get(url, data)
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

    
# Run the FastAPI app
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)