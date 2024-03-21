from fastapi import APIRouter, Request, HTTPException,Body
import requests 
from typing import Any
from globals import *

router = APIRouter()



# reader - <metadb>
@app.put("/update")
async def update_shard(req: Any = Body(...)):
 try:
    acquire_read()
    mysql_conn,mysql_cursor = get_db()

    Stud_id = req["Stud_id"]
    Student = req["data"]
    mysql_cursor.execute("SELECT DISTINCT Shard_id FROM ShardT  WHERE Stud_id_low <= ? AND Stud_id_low + Shard_size > ?",(Stud_id,Stud_id))
    result = mysql_cursor.fetchone()
    print(result)
    if result:
        shard_id = result[0]
        
        #get lock on the shard

        #get all servers that contain the shard
        mysql_cursor.execute("SELECT DISTINCT Server_id FROM MapT WHERE Shard_id = ?",(shard_id,))
        servers = mysql_cursor.fetchall()
        print(servers)
        if servers:
            for server in servers:
                server = server[0]
                payload = {
                    "shard":shard_id,
                    "Stud_id":Stud_id,
                    "data":Student
                }
                result = requests.put(f"http://{server}:8000/update",json=payload,timeout=15)
                
                if not result.ok:
                    raise HTTPException(status_code=500,detail="Internal error")
            return {
                "message": f"Data entry for Stud_id:{Stud_id} updated",
                "status" : "success"
            }
            
        else:
            return {
                "message" : "No server found",
                "status"  : "failure"
            }
 except Exception as e:
    return "some error"
 finally: 
    close_db(mysql_conn,mysql_cursor)
    release_read()