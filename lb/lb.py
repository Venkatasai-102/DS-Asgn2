from random import randint
import uvicorn




from globals import app, mysql_cursor,mysql_conn,MAX_REQUEST_COUNT,MAX_SERVER_INDEX,NUM_SLOTS,VIR_SERVERS
from routers import init, status, add, rm, update, write, read

print("Starting Load Balancer......")


        

# Add routers to the FastAPI app

app.include_router(init.router)
app.include_router(status.router)
app.include_router(add.router)
app.include_router(rm.router)
app.include_router(update.router)
app.include_router(write.router)
app.include_router(read.router)



# Run the FastAPI app
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
