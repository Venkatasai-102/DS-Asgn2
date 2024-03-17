from time import sleep
import requests
from requests.adapters import HTTPAdapter, Retry

from globals import app
from helpers import create_server

def is_alive(server):
    print(f"Checking health of {server} ........")
    try:
        request_url = f"http://{app.server_list[server]['ip']}:{8000}/heartbeat"
        
        req_session = requests.Session()
        retries = Retry(total=3,
                        backoff_factor=0.1,
                        status_forcelist=[ 500, 501, 502, 503, 504 ])

        req_session.mount('http://', HTTPAdapter(max_retries=retries))
        
        response = req_session.get(request_url, timeout=0.5)

        
        print(f'<+> Server {server} is alive')
            
        return True
    except requests.RequestException as e:
        print(f'<!> Server {server} is dead; Error: {e}')
        
        return False



def respawn_dead_server(dead_server):
    
    new_server = f"new_{dead_server}"
    
    print(f"Respawning {dead_server} as {new_server} ......")
    
    old_server_data = app.server_list.pop(dead_server)
    

    name,ipaddr = create_server(name=new_server)
    shards, index = old_server_data["shards"], old_server_data["index"]
    
    # make /config request to the new server
    payload = {
        "schema": app.schema,
        "shards": shards
    }
    
    while True:
        try:
            result = requests.post(f"http://{ipaddr}:{8000}/config", json=payload, timeout=1)
            print(result.ok)
            break
        except requests.RequestException as e:
            print("trying again")
            sleep(1)
    
    app.server_list[new_server] = {
        "index": index,
        "ip": ipaddr,
        "shards": shards
    }
    
    
    print(f"New server {new_server} is created with IP {ipaddr}!")
    
    print(f"Copying shard data from other servers ....... ", end=" ")
    
    for sh in shards:
        # changing the hash info
        app.hash_dict[sh].remove_server(index)
        app.hash_dict[sh].add_server(index, ipaddr, 8000)
        
        # Remove the shard - old server mapping from database       #TODO
        
        
        # get another server containing this shard from database & copy from that server       #TODO
    
    
    
    
    
    
    
    

def check_server_health():
    while 1:
        # Lock server_list
        server_names = list(app.server_list.keys())
        for server in server_names:
            if not is_alive(server):
                respawn_dead_server(server)
        # Unlock server_list
        
        sleep(15)
            
            
            