from fastapi import APIRouter
from fastapi.responses import JSONResponse
from globals import app, mysql_cursor

router = APIRouter()

@app.get("/status")
async def get_status():
    try:
        get_shards_query = "SELECT Stud_id_low, Shard_id, Shard_size FROM ShardT"
        mysql_cursor.execute(get_shards_query)
        shard_rows = mysql_cursor.fetchall()
        
        servers = {}
        for ser in app.server_list:
            get_servers = "SELECT Shard_id FROM MapT WHERE Server_id=?"
            mysql_cursor.execute(get_servers, (ser, ))
            sh_rows = mysql_cursor.fetchall()
            servers[ser] = [sh[0] for sh in sh_rows]
        
        return {
            "N": len(app.server_list), # number of servers currently running
            "schema": {
                "columns":["Stud_id","Stud_name","Stud_marks"],
                "dtypes":["Number","String","String"]
            },
            "shards": [
                {col: value for col, value in zip(["Stud_id_low", "Shard_id", "Shard_size"], row)}
                for row in shard_rows
            ],
            "servers": servers
        }
    except Exception as e:
        print("Exception:", e)
        return JSONResponse(status_code=500, content={"message": "Internal Server Error"})