# Distributed Systems Assignment 2

## Server

- The server containers handle 2 different shards (one replica each) 
- This has the following endpoints:
    - `Endpoint (/config, method=POST)`
    - `Endpoint (/heartbeat, method=GET)`
    - `Endpoint (/copy, method=GET)`
    - `Endpoint (/read, method=POST)`
    - `Endpoint (/write, method=POST)`
    - `Endpoint (/update, method=POST)`
    - `Endpoint (/del, method=DELETE)`

## Loadbalancer

- The loadbalancer stores the information regarding each shard
- The mapping of replica of shard to server
- This has the following endpoints:
    - `Endpoint (/init, method=POST)`
    - `Endpoint (/status, method=GET)`
    - `Endpoint (/add, method=POST)`
    - `Endpoint (/rm, method=POST)`
    - `Endpoint (/read, method=GET)`
    - `Endpoint (/write, method=POST)`
    - `Endpoint (/update, method=PUT)`
    - `Endpoint (/del, method=DELETE)`
- If any server is down the loadbalancer spawns another server

## Client

- Client sends the requests to the loadbalancer.
- Client recieves response from the server via loadbalancer.

## Run Locally
Ensure docker, docker-compose and jupyter-notebook are installed.

Clone the project

```bash
  git clone https://github.com/Venkatasai-102/DS-Asgn2.git
```

Go to the project directory

```bash
  cd DS-Asgn2
```

Install dependencies (For Analysis part)  


```bash
  pip3 install requests, aiohttp, matplotlib, 
```

Build docker images for Loadbalancer and Server

```bash
  sudo make build
```
Start the Loadbalancer

```bash
  sudo make 
```
Now run the code from **analysis.ipynb**z (covers task A1, A2, A3) and **analysis-task-A4.ipynb** (covers task A4)

Remove all containers

```bash
  sudo make clean
```