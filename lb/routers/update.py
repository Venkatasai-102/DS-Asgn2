from fastapi import APIRouter, Request, HTTPException
import requests 

from globals import *

router = APIRouter()




@app.put("/update")
async def update_shard(request: Request):
    req = await request.json()
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
    