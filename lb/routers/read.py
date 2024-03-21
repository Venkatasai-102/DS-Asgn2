
from fastapi import APIRouter, Request,Body
from fastapi.responses import JSONResponse
import requests
from random import randint
from typing import Any
import threading
from globals import *

router = APIRouter()

# reader - <app.server_list,metaDB>
@app.post("/read")
def read_data(req: Any = Body(...)):
    try:
        acquire_read()
        mysql_conn,mysql_cursor = get_db()


        print(f"Thread id: {threading.get_ident()}")
        stud_low = req["Stud_id"]["low"]
        stud_high = req["Stud_id"]["high"]
        # need to optimize to selectively get the shard entries instead of getting all shards in ShardT
        get_shards_query = "SELECT * from ShardT"
        mysql_cursor.execute(get_shards_query)
        response = mysql_cursor.fetchall()
        request_id = randint(1, MAX_REQUEST_COUNT)%NUM_SLOTS

        shards_queried = []

        valid_rows = []
        for row in response:
            if not (stud_low >= row[0]+row[2] or row[0] > stud_high):
                valid_rows.append(row)
                shards_queried.append(row[1])

        result = []

        for row in valid_rows:
            consistent_hashing_object = app.hash_dict[row[1]]
            server = consistent_hashing_object.get_nearest_server(request_id)

            url = f"http://{server.server_ip}:{8000}/read"
            data = {
                "shard": row[1],
                "Stud_id": {
                    "low": max(row[0], stud_low),
                    "high": min(row[0]+row[2], stud_high)
                }
            }

            resp = requests.post(url, json=data)
            
            if resp.status_code != 200:
                return JSONResponse(
                    status_code=400,
                    content={
                        "message": "Invalid status code",
                        "status": "failure"
                    }
                )
            
            resp = resp.json()

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
    except Exception as e:
        return "Some error"
    finally:
        close_db(mysql_conn,mysql_cursor)
        release_read()
