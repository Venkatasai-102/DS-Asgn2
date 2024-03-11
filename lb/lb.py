from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse,RedirectResponse
import subprocess
from random import randint
import mysql.connector as conn
import os
import time
from consistent_hashing import ConsistentHashing
import requests

while True:
    try:
        mysql_conn = conn.connect(
            host=os.getenv("MYSQL_HOST"),
            user=os.getenv("MYSQL_USER", "user"),
            password=os.getenv("MYSQL_PASSWORD", "Pass@123"),
            database=os.getenv("MYSQL_DATABASE", "StudentDB"),
        )
        break
    
    except Exception as e:
        time.sleep(0.02)

mysql_cursor = mysql_conn.cursor()


app = FastAPI()

MAX_REQUEST_COUNT = 1e6
MAX_SERVER_INDEX = 1024
NUM_SLOTS = 512
VIR_SERVERS = 9


# maps for storing locally for loadbalancer
app.hash_dict = {}
app.server_list = {}


def create_server(server_name):
    command = "docker run --name {container_name} --env SERVER_ID={container_name} \
            --network {network_name}  -d serverimg".format(container_name=server_name, network_name="my-net")

    result = subprocess.run(command, shell=True, text=True)
    print(result.returncode)
    
    if result.returncode == 0:
        ipcommand = [
            'docker',
            'inspect',
            '--format={{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}',
            server_name
        ]
        ip = subprocess.run(
            ipcommand, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True, text=True)
        ipaddr = ip.stdout.strip()        
        return {'status': 'success', 'ipaddr': ipaddr, 'name': server_name }
    
    else:
        return {'status': 'failure',
                'response': JSONResponse(
            status_code=500,
            content={
                "message": {
                    "N": len(list(app.server_list.keys())),
                    "replicas": list(app.server_list.keys()),
                    "error": f"failed to create server: {server_name}"
                },
                "status": "failure"
            }
            )
        }

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
        data = {
            "schema": schema,
            "shards": [sh["Shard_id"] for sh in shards]
        }

        requests.post(url, data)
            
        # on success
        app.serverList[server_name] = {"index": randint(1, MAX_SERVER_INDEX), "ip": result['ipaddr']}
        for sh in servers[server_name]:
            if sh not in app.hash_dict:
                app.hash_dict[sh] = ConsistentHashing(NUM_SLOTS, VIR_SERVERS)
            
            app.hash_dict[sh].add_server(app.serverList[server_name]['index'], result['ipaddr'], 8000)
            
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
        app.serverList[server_name] = {"index": randint(1, MAX_SERVER_INDEX), "ip": result['ipaddr']}
        for sh in servers[server_name]:
            if sh not in app.hash_dict:
                app.hash_dict[sh] = ConsistentHashing(NUM_SLOTS, VIR_SERVERS)
            
            app.hash_dict[sh].add_server(app.serverList[server_name]['index'], result['ipaddr'], 8000)

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