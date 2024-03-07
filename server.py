from fastapi import FastAPI, Request, HTTPException
import os
from dotenv import load_dotenv
import mysql.connector as conn

# Load environment file
load_dotenv()

app = FastAPI()

server_id = os.getenv("SERVER_ID")

# Connect to MySQL
mysql_conn = conn.connect(
    host=os.getenv("MYSQL_HOST"),
    user=os.getenv("MYSQL_USER"),
    password=os.getenv("MYSQL_PASSWORD"),
    database=os.getenv("MYSQL_DATABASE"),
    port=os.getenv("MYSQL_PORT")
)

mysql_cursor = mysql_conn.cursor()

@app.get("/heartbeat")
async def heartbeat():
    return ""

# Initialzes the shards
@app.post("/config")
async def initialize_shards(request: Request):
    try:
        req = await request.json()
        # schema is a dictionary with columns as stud_id, stud_name, stud_marks and 
        # dtypes as number, string, string
        schema = req["schema"]

        # shards is a list of shard names
        shards = req["shards"]

        message = ""

        # Create a table for each shard
        for i, shard in enumerate(shards):
            # Hardcoded schema for now since given data-types are wrong
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {shard} (
                {schema["columns"][0]} INT PRIMARY KEY,
                {schema["columns"][1]} VARCHAR(255),
                {schema["columns"][2]} VARCHAR(4)
            )
            """

            # Insert schema
            mysql_cursor.execute(create_table_query)
            if i == 0:
                message += f"{server_id}:{shard}"
            else:
                message += f", {server_id}:{shard}"

        message += " configured"
        # Commit changes
        mysql_conn.commit()

        return {"message": message, "status": "success"}

    except conn.Error as err:
         # In case of an error, rollback changes and raise an exception
        mysql_conn.rollback()
        raise HTTPException(status_code=500, detail=f"An error occurred: {err}")
    
    except:
        raise HTTPException(status_code=400, detail="Invalid request")
    
@app.get("/copy")
async def get_all_shards_data(request: Request):
    try:
        req = await request.json()
        shards = req["shards"]

        # Get all data from each shard
        response = {}
        for shard in shards:
            mysql_cursor.execute(f"SELECT * FROM {shard}")
            response[shard] = mysql_cursor.fetchall()

        response["status"] = "success"
        
        return response

    except conn.Error as err:
        raise HTTPException(status_code=500, detail=f"An error occurred: {err}")
    
    except:
        raise HTTPException(status_code=400, detail="Invalid request")

@app.get("/read")
async def get_students_data(request: Request):
    try:
        req = await request.json()
        shard = req["shard"]
        id_range = req["Stud_id"]
        low = id_range["low"]
        high = id_range["high"]

        # Get data from the shard
        mysql_cursor.execute(f"SELECT * FROM {shard} WHERE Stud_id >= {low} AND Stud_id <= {high}")
        response = {}
        response["data"] = mysql_cursor.fetchall()

        response["status"] = "success"

        return response
    
    except conn.Error as err:
        raise HTTPException(status_code=500, detail=f"An error occurred: {err}")
    
    except:
        raise HTTPException(status_code=400, detail="Invalid request")

@app.post("/write")
async def add_students_data(request: Request):
    try:
        req = await request.json()
        shard = req["shard"]
        curr_idx = req["curr_idx"]
        data = req["data"]

        # Add data to the shard
        for row in data:
            mysql_cursor.execute(f"INSERT INTO {shard} VALUES ({row['Stud_id']}, '{row['Stud_name']}', '{row['Stud_marks']}')")

        # Commit changes
        mysql_conn.commit()

        # Update the current index
        curr_idx += len(data)

        response = {
            "message": "Data entries added",
            "current_idx": curr_idx,
            "status" : "success"
        }

        return response

    except conn.Error as err:
        # In case of an error, rollback changes and raise an exception
        mysql_conn.rollback()
        raise HTTPException(status_code=500, detail=f"An error occurred: {err}")
    
    except:
        raise HTTPException(status_code=400, detail="Invalid request")

@app.put("/update")
async def update_student_data(request: Request):
    try:
        req = await request.json()
        shard = req["shard"]
        stud_id = req["Stud_id"]
        data = req["data"]
        stud_name = data["Stud_name"]
        stud_marks = data["Stud_marks"]

        # Update the data
        mysql_cursor.execute(f"UPDATE {shard} SET Stud_name = '{stud_name}', Stud_marks = '{stud_marks}' WHERE Stud_id = {stud_id}")

        # Commit changes
        mysql_conn.commit()

        response = {
            "message": f"Data entry for Stud_id:{stud_id} updated",
            "status" : "success"
        }

        return response
    except conn.Error as err:
        # In case of an error, rollback changes and raise an exception
        mysql_conn.rollback()
        raise HTTPException(status_code=500, detail=f"An error occurred: {err}")
    
    except:
        raise HTTPException(status_code=400, detail="Invalid request")

@app.delete("/del")
async def delete_student_data(request: Request):
    try:
        req = await request.json()
        shard = req["shard"]
        stud_id = req["Stud_id"]

        # Delete the data
        mysql_cursor.execute(f"DELETE FROM {shard} WHERE Stud_id = {stud_id}")

        # Commit changes
        mysql_conn.commit()

        response = {
            "message": f"Data entry for Stud_id:{stud_id} removed",
            "status" : "success"
        }

        return response
    
    except conn.Error as err:
        # In case of an error, rollback changes and raise an exception
        mysql_conn.rollback()
        raise HTTPException(status_code=500, detail=f"An error occurred: {err}")
    
    except:
        raise HTTPException(status_code=400, detail="Invalid request")

# Run the FastAPI app
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)