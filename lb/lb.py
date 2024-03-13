from random import randint
import uvicorn
import subprocess
import os
import time
from consistent_hashing import ConsistentHashing
import requests
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse,RedirectResponse
import mysql.connector as conn
from helpers import create_server,remove_servers,get_servers

print("Starting Load Balancer......")

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
        # print(e)
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


@app.post("/init")
async def init_system(request: Request):
    req = await request.json()
    n, schema = req['N'], req['schema']
    shards, servers = req['shards'], req['servers']
    print(servers)
    # if n != len(shards) or n != len(servers):
    #     return JSONResponse(
    #         status_code=400,
    #         content={
    #             "message": f"value of N and number of shards/servers don't match!",
    #             "status": "failure"
    #         }
    #     )
        
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
        
        # need to change to the network name currently put as localhost for testing
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
                # print(e)
                print("trying again")
                time.sleep(0.5)
    
        # on success
        app.server_list[server_name] = {"index": randint(1, MAX_SERVER_INDEX), "ip": ip[server_name]}
        for sh in servers[server_name]:
            if sh not in app.hash_dict:
                app.hash_dict[sh] = ConsistentHashing(NUM_SLOTS, VIR_SERVERS)
            
            app.hash_dict[sh].add_server(app.server_list[server_name]['index'], ip[server_name], 8000)
            
            ## add shard-server mapping to database
            add_mapt_query = "INSERT INTO MapT VALUES (%s, %s)"
            print(add_mapt_query)
            try:
                mysql_cursor.execute(add_mapt_query,(sh,server_name))
                mysql_conn.commit()
            except Exception as e:
                # print(e)
                print("Issue is here")

    # creating all shard entries in ShardT
    for shard in shards:
        shard_query = "INSERT INTO ShardT VALUES (%s,%s,%s,%s)"

        try:
            mysql_cursor.execute(shard_query,(shard["Stud_id_low"],shard["Shard_id"],shard["Shard_size"],shard["Stud_id_low"]))
            mysql_conn.commit()
        except Exception as e:
                print(e)
                print("Issue is here ):")

        
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
    
    for server_name in servers:
        # on success, adding this new server to the respective shards' consistent hashing object
        app.server_list[server_name] = {"index": randint(1, MAX_SERVER_INDEX), "ip": ip[server_name]}
        for sh in servers[server_name]:
            if sh not in app.hash_dict:
                app.hash_dict[sh] = ConsistentHashing(NUM_SLOTS, VIR_SERVERS)
            
#             app.hash_dict[sh].add_server(app.server_list[server_name]['index'], result['ipaddr'], 8000)

            # need to handle the case when one of the server fails, we need to stop the already created servers or do something else
            # adding entry in MapT

            add_mapt_query = "INSERT INTO MapT VALUES (%s,%s)"
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
                time.sleep(0.5)
                

        
    for shard in new_shards:
        shard_query ="INSERT INTO ShardT VALUES (%s,%s,%s,%s)"
        mysql_cursor.execute(shard_query,(shard["Stud_id_low"],shard["Shard_id"],shard["Shard_size"],shard["Stud_id_low"]))
        mysql_conn.commit()