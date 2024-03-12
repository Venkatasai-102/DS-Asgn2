import docker
from typing import Tuple,List
client = docker.from_env()

def create_server(image:str,network:str ="mynet",name:str =None):
    container = client.containers.run(image=image,network=network,name=name,detach=True)
    container.reload()
    name = container.name
    ipaddr = get_ipaddr(container,network)
    return name,ipaddr

def remove_servers(servers):
    for server in servers:
            client.containers.get(server).remove(force=True)
    

def get_ipaddr(container,network)->str:
    return container.attrs['NetworkSettings']["Networks"][network]['IPAddress']

def get_servers(image:str,network:str="mynet"):
    serverList = []
    for container in client.containers.list(filters={"ancestor":image}):
        serverList.append((container.name,get_ipaddr(container,network)))
    return serverList
