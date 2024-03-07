import hashlib

class ServerNode:
    def __init__(self, i, j, ip, port):
        self.server_id_i = i
        self.server_id_j = j
        self.server_ip = ip
        self.server_port = port


class RequestNode:
    def __init__(self):
        self.req = {}


class Node:
    def __init__(self):
        self.server = None
        self.requests = []
        # `next` helps in storing the next virtual server and this is dependent on number of slots but
        # not on number of requests which is an optimization
        self.next = None


# Consistent hashing parameters
# num_servers = 3
# num_slots = 512
# vir_servers = 9


class ConsistentHashing:
    # Array that holds virtual servers and requests
    def __init__(self,num_slots,vir_servers) -> None:
        self.circular_array = []
        for _ in range(num_slots):
            self.circular_array.append(Node())
        self.num_slots = num_slots
        self.vir_servers = vir_servers

    # Request mapping hash function
    def request_hash(self,i) -> int:
        '''
            Choose the hash function here for testing purpose
            between the two hash functions
        '''
        # return (i**2 + 2 * i + 17) % self.num_slots
        return int(hashlib.sha256(str(i).encode()).hexdigest(), 16) % self.num_slots

    # Server mapping hash function
    def server_hash(self,i, j) -> int:
        return (i**2 + j**2 + 2 * i * j + 25) % self.num_slots
    
    def server_hash_sha_256(self, i, j) -> int:
        """
            SHA-256 hash function for mapping
        """
        return int(hashlib.sha256(str(i).encode() + "-".encode() + str(j).encode()).hexdigest(), 16) % self.num_slots

    # Linear Probing for servers in case of collision
    def linear_probe(self,pos) -> int:
        """
        Linearly probes the next available
        slot in the map for the virtual server
        """
        count = 0
        while self.circular_array[pos].server is not None:
            pos = (pos + 1) % self.num_slots
            count = count + 1
            if count == self.num_slots:
                return -1

        return pos

    # Finds the nearest server
    def find_nearest_server(self,pos) -> int:
        """
        Finds the position of nearest virtual server
        in the clockwise direction
        """
        j = (pos + 1) % self.num_slots
        cntr = 0
        while True:
            if cntr == self.num_slots:
                return j
            
            if self.circular_array[j].server is not None:
                return j

            j = (j + 1) % self.num_slots
            cntr += 1
    
    # Gets nearest server node given a position
    def get_nearest_server(self, pos) -> ServerNode:
        '''
            Gets the nearest server in the the clockwise direction,
            given the position of reqest 
        '''
        j = (pos + 1) % self.num_slots
        while True:
            if self.circular_array[j].server is not None:
                return self.circular_array[j].server

            j = (j + 1) % self.num_slots

    # Map server to request
    def map_server_to_request(self,pos):
        """
        Maps the requests to this virtual server and
        traverses in anti-clockwise direction
        """
        j = (pos - 1 + self.num_slots) % self.num_slots
        while self.circular_array[j].server is None:
            self.circular_array[j].next = pos
            j = (j - 1 + self.num_slots) % self.num_slots

    # Add Server
    def add_server(self, i, ip, port):
        """
        Adds virtual servers to the map, and in
        case of collision, linearly probes it also
        maps the requests to this new virtual server
        accordingly
        """
        for j in range(self.vir_servers):
            '''
                Choose the hash function here for testing purpose
            '''
            # pos = self.linear_probe(self.server_hash(i, j))
            pos = self.linear_probe(self.server_hash_sha_256(i, j))
            if pos == -1:
                return "Slots are full cannot add new server"
            self.circular_array[pos].server = ServerNode(i, j, ip, port)
            self.map_server_to_request(pos)

    # Remove Server
    def remove_server(self, i):
        """
        Removes all the virtual server instances from
        the map and re-maps the requests that are pointed to
        this to the next closest virtual server in the
        clockwise direction
        """
        for j in range(self.vir_servers):
            '''
                Choose the hash function here for testing purpose
            '''
            # pos = self.server_hash(i, j)
            pos = self.server_hash_sha_256(i, j)
            while (
                self.circular_array[pos].server is None
                or self.circular_array[pos].server.server_id_i != i
            ):
                pos = (pos + 1) % self.num_slots

            self.circular_array[pos].server = None
            next_pos = self.find_nearest_server(pos)
            self.circular_array[pos].next = next_pos

            # re-map requests
            k = (pos - 1 + self.num_slots) % self.num_slots
            while self.circular_array[k].server is not None:
                self.circular_array[k].next = next_pos
                k = (k - 1 + self.num_slots) % self.num_slots
