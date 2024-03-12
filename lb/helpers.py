import docker
from docker import types
from typing import Tuple,List
client = docker.from_env()

def create_server(image:str="serverimg",network:str ="mynet",name:str =None)->Tuple[str,str]:
     """
        Creates a server container based on the provided image within a specified network.

        Args:
        - image (str): The name of the Docker image to create the container from.
        - network (str): The name of the Docker network to connect the container to. Defaults to "mynet".
        - name (str): The name to assign to the container. If not provided, Docker assigns a random name.

        Returns:
        - Tuple[str, str]: A tuple containing the name and IP address of the created container.
     """
    #  docker.types
     container = client.containers.run(image=image,network=network,name=name,detach=True)
     container.reload()
     name = container.name
     ipaddr = get_ipaddr(container,network)
     return name,ipaddr

def remove_servers(servers):
    """
    Removes Docker containers corresponding to the given server names.

    Args:
        servers (list): List of strings representing server names.

    Returns:
        None
    """
    for server in servers:
            client.containers.get(server).remove(force=True)
    

def get_ipaddr(container,network)->str:
    return container.attrs['NetworkSettings']["Networks"][network]['IPAddress']

def get_servers(image:str,network:str="mynet"):
    """
    Retrieves a list of server containers

    Args:
        image (str): Name of the Docker image.
        network (str, optional): Name of the Docker network. Defaults to "mynet".

    Returns:
        list: List of  containing names .
    """
    serverList = []
    for container in client.containers.list(filters={"ancestor":image}):
        serverList.append((container.name,get_ipaddr(container,network)))
    return serverList

