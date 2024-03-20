from fastapi import APIRouter, Request,Body
from fastapi.responses import JSONResponse
import requests
from requests import RequestException
from typing import Any
from globals import *
import threading
router = APIRouter()



@app.post("/write")
def write(req: Any=Body(...)):
    # need to map shard to server
    #print thread id  
    print(f"Thread id: {threading.get_ident()}")
    try:
        students = req["data"]
        print(students)

        GET_SHARDS_QUERY = "SELECT * FROM ShardT"
        mysql_cursor.execute(GET_SHARDS_QUERY)
        rows = mysql_cursor.fetchall()
        shards = {row[1]: {"students":[],"attr":list(row),"server":[]} for row in rows}
        mysql_cursor.execute("SELECT * FROM MapT")
        MapT_rows =mysql_cursor.fetchall()
        
        for MapT_row in MapT_rows:
            shard_id = MapT_row[0]
            server = MapT_row[1]
            shards[shard_id]["server"].append(server)

        print(shards)
        for student in students:
            Stud_id =student["Stud_id"]
            Stud_name=student["Stud_name"]
            Stud_marks=student["Stud_marks"]
            
            for shard_id in shards:
                if shards[shard_id]["attr"][0] <= Stud_id and Stud_id <= shards[shard_id]["attr"][0]+shards[shard_id]["attr"][2]:
                    shards[shard_id]["students"].append((Stud_id,Stud_name,Stud_marks))
        
        data_written = []
        for shard_id in shards:
            # acquire the lock for this shard

            queries = [{"Stud_id":stud[0],"Stud_name":stud[1],"Stud_marks":stud[2]} for stud in shards[shard_id]["students"]]
            data= { "shard":shard_id,"curr_idx":shards[shard_id]["attr"][3] ,"data":queries}
            curr_idx = None
            for server in shards[shard_id]["server"]:
                print(f"Sending request to {server} :{shard_id}")
                result = requests.post(f"http://{server}:8000/write",json=data,timeout=15)
                if result.status_code != 200:
                    return JSONResponse(status_code=400,content={
                        "message":f"writes to shard {shard_id} failed",
                        "data entries written successfully":data_written,
                        "status":"failure"
                    })
                print(result.json())
                curr_idx = result.json()["current_idx"]
            shards[shard_id]["attr"][3]=curr_idx
            mysql_cursor.execute("UPDATE ShardT SET valid_idx= ? WHERE Stud_id_low = ? AND Shard_id = ?",(curr_idx,shards[shard_id]["attr"][0],shard_id))
            mysql_conn.commit()
            data_written.extend(queries)
        
        return {"message":f"{len(students)} Data entries added","status":"success"}
        
    except RequestException as e:
        print("RequestException:", e)
        return JSONResponse(status_code=500, content={"message": "Request failed", "status": "failure"})
        
    except sqlite3.Error as e: 
        print("SQLite Error:", e)
        return JSONResponse(status_code=500, content={"message": "SQLite error", "status": "failure"})
        
    except Exception as e:
        print("Other Exception:", e)
        return JSONResponse(status_code=500, content={"message": "Unexpected error", "status": "failure"})