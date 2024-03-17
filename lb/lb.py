import uvicorn
from time import sleep
from threading import Thread


from globals import app
from routers import init, status, add, rm, update, write, read, delete

from crash_handler import check_server_health




        

# Add routers to the FastAPI app

app.include_router(init.router)
app.include_router(status.router)
app.include_router(add.router)
app.include_router(rm.router)
app.include_router(update.router)
app.include_router(write.router)
app.include_router(read.router)
app.include_router(delete.router)




# def checker_thread():
#     while 1:
#         print("Checking server health ....")
#         sleep(5)
    


# Run the FastAPI app
if __name__ == "__main__":
    
    print("Starting Load Balancer......")
    
    # routing_thread()
    
    t1 = Thread(target=lambda: uvicorn.run(app, host="0.0.0.0", port=8000))
    t2 = Thread(target=check_server_health)
    
    t2.start()
    t1.start()
    
    t1.join()
    t2.join()
    
    
    
