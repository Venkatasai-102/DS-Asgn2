import uvicorn



from globals import app
from routers import init, status, add, rm, update, write, read, delete

print("Starting Load Balancer......")


        

# Add routers to the FastAPI app

app.include_router(init.router)
app.include_router(status.router)
app.include_router(add.router)
app.include_router(rm.router)
app.include_router(update.router)
app.include_router(write.router)
app.include_router(read.router)
app.include_router(delete.router)



# Run the FastAPI app
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
