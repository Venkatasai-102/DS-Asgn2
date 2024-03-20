from fastapi import APIRouter, Request, HTTPException,Body
from fastapi.responses import JSONResponse
from requests import RequestException
from docker import errors
from typing import Any
from helpers import remove_servers
from globals import *

router = APIRouter()




@app.delete("/rm")
def rm_servers(req: Any=Body(...)):
    try:
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
