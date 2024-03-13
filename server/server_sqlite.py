from fastapi import FastAPI, Request, HTTPException
import os
from dotenv import load_dotenv
import sqlite3
import time

# Load environment file
load_dotenv()

app = FastAPI()

server_id = os.getenv("SERVER_ID", "Server0")

# Connect to SQLite database
conn = sqlite3.connect('student.db')
cursor = conn.cursor()



@app.on_event("shutdown")
async def shutdown_event():
    # Close SQLite connection
    print("Cleaning up the resources")
    conn.close()

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
        shards = req["shards"]
        message = ""

        # Create a table for each shard
        for i, shard in enumerate(shards):
            # Hardcoded schema for now since given data-types are wrong
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {shard} (
                {schema["columns"][0]} INTEGER PRIMARY KEY,
                {schema["columns"][1]} TEXT,
                {schema["columns"][2]} TEXT
            )
            """
            cursor.execute(create_table_query)
            if i == 0:
                message += f"{server_id}:{shard}"
            else:
                message += f", {server_id}:{shard}"

        message += " configured"
        conn.commit()
        return {"message": message, "status": "success"}

    except sqlite3.Error as err:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"An error occurred: {err}")

    except Exception as e:
        raise HTTPException(status_code=400, detail="Invalid request")

@app.get("/copy")
async def get_all_shards_data(request: Request):
    try:
        req = await request.json()
        shards = req["shards"]

        response = {}
        for shard in shards:
            cursor.execute(f"SELECT * FROM {shard}")
            rows = cursor.fetchall()
            response[shard] = [
                {col: value for col, value in zip(['Stud_id', 'Stud_name', 'Stud_marks'], row)}
                for row in rows
            ]
        response["status"] = "success"
        return response

    except sqlite3.Error as err:
        raise HTTPException(status_code=500, detail=f"An error occurred: {err}")

    except:
        raise HTTPException(status_code=400, detail="Invalid request")

@app.post("/read")
async def get_students_data(request: Request):
    try:
        req = await request.json()
        shard = req["shard"]
        id_range = req["Stud_id"]
        low = id_range["low"]
        high = id_range["high"]

        cursor.execute(
            f"SELECT * FROM {shard} WHERE Stud_id >= {low} AND Stud_id <= {high}")
        response = {}
        response["data"] = cursor.fetchall()

        response["status"] = "success"

        return response

    except sqlite3.Error as err:
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

        for row in data:
            cursor.execute(
                f"INSERT INTO {shard} (Stud_id, Stud_name, Stud_marks) VALUES (?, ?, ?)", 
                (row['Stud_id'], row['Stud_name'], row['Stud_marks'])
            )

        conn.commit()
        curr_idx+=len(data)

        response = {
            "message": "Data entries added",
            "current_idx": curr_idx,
            "status": "success"
        }

        return response

    except sqlite3.Error as err:
        conn.rollback()
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

        cursor.execute(
            f"UPDATE {shard} SET Stud_name = ?, Stud_marks = ? WHERE Stud_id = ?", 
            (stud_name, stud_marks, stud_id)
        )

        conn.commit()

        response = {
            "message": f"Data entry for Stud_id:{stud_id} updated",
            "status": "success"
        }

        return response
    except sqlite3.Error as err:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"An error occurred: {err}")

    except:
        raise HTTPException(status_code=400, detail="Invalid request")

@app.delete("/del")
async def delete_student_data(request: Request):
    try:
        req = await request.json()
        shard = req["shard"]
        stud_id = req["Stud_id"]

        cursor.execute(f"DELETE FROM {shard} WHERE Stud_id = ?", (stud_id,))

        conn.commit()

        response = {
            "message": f"Data entry for Stud_id:{stud_id} removed",
            "status": "success"
        }

        return response

    except sqlite3.Error as err:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"An error occurred: {err}")

    except:
        raise HTTPException(status_code=400, detail="Invalid request")

# Run the FastAPI app
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
