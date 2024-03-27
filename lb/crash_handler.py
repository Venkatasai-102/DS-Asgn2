from time import sleep
import requests
from requests.adapters import HTTPAdapter, Retry
from globals import app,get_db,close_db,acquire_write,release_write
from helpers import create_server
import sqlite3
import requests


CHECK_INTERVAL = 10 # 1 minute

def is_alive(server):
    print(f"Checking health of {server} ........",flush=True)
    try:
        request_url = f"http://{app.server_list[server]['ip']}:{8000}/heartbeat"
        
        req_session = requests.Session()
        retries = Retry(total=3,
                        backoff_factor=0.1,
                        status_forcelist=[ 500, 501, 502, 503, 504 ])

        req_session.mount('http://', HTTPAdapter(max_retries=retries))
        
        response = req_session.get(request_url, timeout=0.5)

        
        print(f'<+> Server {server} is alive',flush=True)
            
        return True
    except requests.RequestException as e:
        print(f'<!> Server {server} is dead; Error: {e}',flush=True)
        
        return False



def respawn_dead_server(dead_server,conn,cursor):
    
    new_server = f"new_{dead_server}"
    
    print(f"Respawning {dead_server} as {new_server} ......",flush=True)
    
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
    
    
    print(f"New server {new_server} is created with IP {ipaddr}!",flush=True)
    
    print(f"Copying shard data from other servers ....... ", end=" ",flush=True)
    print(shards,flush=True)
    for sh in shards:
        # changing the hash info
        print(f".....Restoring {sh} ",flush=True)
        app.hash_dict[sh].remove_server(index)
        app.hash_dict[sh].add_server(index, ipaddr, 8000)

        # Remove the shard - old server mapping from database       #TODO
        cursor.execute("DELETE FROM MapT WHERE Server_id=? AND Shard_id=?",(dead_server,sh))
        conn.commit()
        # get another server containing this shard from database & copy from that server       #TODO
        cursor.execute("SELECT DISTINCT Server_id FROM MapT WHERE Shard_id=?",(sh,))
        server_sh = cursor.fetchall()
        students = None
        server_id = None
        for __server in server_sh:
        # now make a req to    /copy endpoint
            try:
                server_id = __server[0]
                resp = requests.get(f"http://{app.server_list[server_id]['ip']}:8000/copy",json={
                    "shards": [sh]
                },timeout=15)
                students = resp.json()[sh]
                break
            except requests.RequestException as e:
                print(f"Request to {server_id} failed",flush=True)
                print("Trying with another server",flush=True)

        print("=== Student List ===",flush=True)
        print(students,flush=True)
        print("====================",flush=True)

            # copy the shard data to the newly spawned server 
        requests.post(f"http://{ipaddr}:8000/write",json={
            "shard":sh,
            "curr_idx": 0, 
            "data": students
        },timeout=15)
        print(f"Successfully copied shard:{sh} data from ", server_id," to ", new_server,flush=True) 
        # add the shard - new server mapping to database
        cursor.execute("INSERT INTO MapT VALUES(?,?)",(sh,new_server))
        conn.commit()
        print(f"Successfully inserted shard:{sh} to server:{new_server} mapping into MapT",flush=True )
        
        # clean up down by check_server_health


    print("Done!",flush=True)
    
    
    
    
    
    

def check_server_health():
    while 1:
        try:

            # acquire write lock
            acquire_write()
            print("Checking server health ....",flush=True)
            server_names = list(app.server_list.keys())
            print("Server names: ",flush=True)
            print(server_names) 
            conn,cursor = get_db()
            for server in server_names:
                if not is_alive(server):
                    print(app.server_list)
                    respawn_dead_server(server,conn,cursor)

        except Exception as e:
            print(f"Error: {e}",flush=True)

        finally:
            close_db(conn,cursor)
            print("finished checking server health",flush=True)
            release_write()  # release write lock
        
        sleep(CHECK_INTERVAL)
            
            
            
