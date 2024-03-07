from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse,RedirectResponse
import subprocess
from random import randint

from consistent_hashing import ConsistentHashing


app = FastAPI()

MAX_REQUEST_COUNT = 1e6
MAX_SERVER_INDEX = 1024
NUM_SLOTS = 512
VIR_SERVERS = 9


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
        return {'status': 'success', 'ip': ipaddr, 'name': server_name }
    
    else:
        return {'status': 'failure',
                'response': JSONResponse(
            status_code=500,
            content={
                "message": {
                    "N": len(list(app.serverList.keys())),
                    "replicas": list(app.serverList.keys()),
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
        
    # need to store the shards map with server in database
    # need to create ShardT schema in database
    
    shard_names = []
    for shard in shards:
        shard_names.append(shard['Shard_id'])
    
    for server_name in servers:
        result = create_server(server_name)
        
        if result['status'] == 'failure':
            return result['response']
            
        # on success
        app.serverList[server_name] = {"index": randint(1, MAX_SERVER_INDEX), "ip": result['ipaddr']}
        for sh in servers[server_name]:
            if sh not in app.hash_dict:
                app.hash_dict[sh] = ConsistentHashing(NUM_SLOTS, VIR_SERVERS)
            
            app.hash_dict[sh].add_server(app.serverList[server_name]['index'], result['ipaddr'], 8080)
                
        
        
        
        
    
    
    
    
    

