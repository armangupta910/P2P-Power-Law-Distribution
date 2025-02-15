import socket
import threading
import time
import hashlib 
import random
from queue import Queue   

no_of_threads = 3             # We have 3 threads each for listen, liveness testing and gossip
job_no = [1, 2, 3]            # We will create 3 jobs in queue for running each thread  
queue = Queue()               # Queue to store our jobs  

MY_IP = "0.0.0.0"                                          # MY_IP will later contain IP Address for listening 
PORT = int(input())                                        # Port for listening 
seeds_addr = set()                                         # To store seed address received from config.txt
peer_set_from_seed = set()                                 # Set used to store different peers address received from seed 
peers_connected = []                                       # To store list of peer objects connected
MessageList = []                                           # To store hash of GOSSIP messages 
connect_seed_addr = []                                     # To store seed address to which peer is connected

# Write the outputs to the file
def write_output_to_file(message):
    try:
        with open("outputpeer.txt", "a") as file:
            if isinstance(message, dict):
                # This is a peer connection message
                output = f"Address: {message['address']}, Degree: {message['degree']}\n"
            elif isinstance(message, str) and message.startswith("Dead Node:"):
                # This is a dead node report
                output = f"{message}\n"
            else:
                # For any other type of message
                output = f"{message}\n"
            file.write(output)
    except IOError:
        print("Write Failed")


# Class of Peer objects 
class Peer: 
    i = 0
    address = ""
    def __init__(self, addr): 
        self.address = addr 

# To find self timestamp
def timestamp():
    time_stamp = time.time()
    return time_stamp

# To generate hash of message
def hash_of_message(message):
    result = hashlib.sha256(message.encode()).hexdigest()
    return result

# Read address of seeds from config file
def read_addr_of_seeds():
    try:
        global seeds_address_list
        file = open("config.txt","r")
        seeds_address_list = file.read()
        print("Seeds Found :- ")
        print(seeds_address_list)
    except:
        print("Read from config failed")
    finally:
        file.close()

# To calculate n i.e. total no. of seeds and also to find set of all available seeds
def total_available_seeds():
    global seeds_address_list
    temp = seeds_address_list.split("\n")
    for addr in temp:
        if addr:
            addr = addr.split(":")
            addr = "0.0.0.0:" + str(addr[1])
            seeds_addr.add(addr)
    return len(seeds_addr)

# Generate k random numbers in a given range.  
def generate_k_random_numbers_in_range(lower, higher, k):
    random_numbers_set = set()
    while len(random_numbers_set) < k:
        random_numbers_set.add(random.randint(lower,higher))
    return random_numbers_set

# Create socket to connect 2 computers
def create_socket():
    try:
        global sock
        sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    except:
        print("Socket Creation Error")

# To Bind Socket
def bind_socket():
    try:
        global sock
        ADDRESS = (MY_IP, PORT)
        sock.bind(ADDRESS)
    except socket.error:
        print("Socket Binding Error")
        bind_socket()

def increment_degree_on_seeds():
    MY_ADDRESS = f"{MY_IP}:{PORT}"
    for seed_addr in connect_seed_addr:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            seed_addr = seed_addr.split(":")
            ADDRESS = (str(seed_addr[0]), int(seed_addr[1]))
            sock.connect(ADDRESS)
            
            increment_message = f"INCREMENT:{MY_ADDRESS}"
            sock.send(increment_message.encode('utf-8'))
            
            response = sock.recv(1024).decode('utf-8')
            print(f"Degree increment response from seed {response}")
            
            sock.close()
        except:
            print(f"Failed to increment degree on seed {seed_addr}")

# To handle different connected peers in different thread.It recieves messages from peer.
# According to the type of message received take appropriate actions
def handle_peer(conn, addr):
    while True:
        try:
            message = conn.recv(1024).decode('utf-8')
            received_data = message
            if message:
                print("Message received at begin :- ", message)
                message = message.split(":")
                if "New Connect Request From" in message[0]:      # If its new connection request then check if already 4 peers are not connected then accept the connection
                    if(len(peers_connected) < 4):
                        conn.send("New Connect Accepted".encode('utf-8'))
                        peers_connected.append( Peer(str(addr[0])+":"+str(message[2])) )
                        increment_degree_on_seeds()
                elif "Liveness Request" in message[0]:            # If its liveness request then give back liveness reply              
                    liveness_reply = "Liveness Reply:" + str(message[1]) + ":" + str(message[2]) + ":" + str(MY_IP) + ":" + str(PORT)
                    conn.send(liveness_reply.encode('utf-8'))
                elif "GOSSIP" in message[3][0:6]:                 # If its gossip message then forward it if its not in ML list
                    forward_gossip_message(received_data)
        except:
            break
    conn.close()

# To listen at a particular port and create thread for each peer                 
def begin():
    sock.listen(5)
    print("Peer is Listening")
    while True:
        conn, addr = sock.accept()
        sock.setblocking(1)
        thread = threading.Thread(target = handle_peer, args = (conn,addr))
        thread.start()
        
# This function receives complete list of peers and set of random index of peers to connect to and connect to them
def connect_peers(selected_peers):
    if(len(selected_peers) == 0):
        print("No peers found to connect")
        return 0
    for peer in selected_peers:
        try:
            # Your existing connection logic here
            # For example:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            ip, port = peer['address'].split(':')
            sock.connect((ip, int(port)))
            
            # Send connection request
            request = f"New Connect Request From:{MY_IP}:{PORT}"
            sock.send(request.encode('utf-8'))
            
            # Wait for response
            response = sock.recv(1024).decode('utf-8')
            
            if response == "New Connect Accepted":
                peers_connected.append(Peer(peer['address']))
                print(f"Successfully connected to {peer['address']}")
                increment_degree_on_seeds()
            else:
                print(f"Connection rejected by {peer['address']}")
            
            sock.close()
        except Exception as e:
            print(f"Failed to connect to {peer['address']}: {str(e)}")

    print(f"Connected to {len(peers_connected)} peers")

def join_atmost_4_peers(complete_peer_list):
    if len(complete_peer_list) > 0:
        # Determine the number of peers to connect to (1 to 4, or less if fewer peers are available)
        limit = min(random.randint(1, len(complete_peer_list)), 4)
        
        # Randomly select peers to connect to
        selected_peers = random.sample(complete_peer_list, limit)
        
        print("Trying to connect to peers:")
        for peer in selected_peers:
            print(f"Address: {peer['address']}, Degree: {peer['degree']}")
        
        return connect_peers(selected_peers)
    else:
        return 0




# It take complete peer list separated by comma from each seed and union them all
def union_peer_lists(complete_peer_list):
    global MY_IP
    global PORT
    global peer_set_from_seed

    # Clear the existing set
    peer_set_from_seed = set()

    # Split the received string into individual peer entries
    peer_entries = complete_peer_list.split(";")

    for entry in peer_entries:
        if entry:
            parts = entry.split(":")
            if len(parts) >= 3:
                address = f"{parts[0]}:{parts[1]}"  # Combine IP and port
                degree = int(parts[2])
                peer_set_from_seed.add((address, degree))

    # Extract MY_IP from the last entry (assuming it's still included)
    if peer_entries:
        last_entry = peer_entries[-1].split(":")
        if len(last_entry) >= 1:
            MY_IP = last_entry[0]

    # Convert the set back to a list of dictionaries
    complete_peer_list = [{"address": addr, "degree": deg} for addr, deg in peer_set_from_seed]

    # Remove self peer
    self_address = f"{MY_IP}:{PORT}"
    complete_peer_list = [peer for peer in complete_peer_list if peer['address'] != self_address]

    return complete_peer_list





def update_degree_to_seeds(new_degree):
    for seed_addr in connect_seed_addr:
        try:
            sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            seed_addr = seed_addr.split(":")
            ADDRESS = (str(seed_addr[0]), int(seed_addr[1]))
            sock.connect(ADDRESS)
            
            MY_ADDRESS = str(MY_IP)+":"+str(PORT)
            update_message = f"UPDATE:{MY_ADDRESS}:{new_degree}"
            sock.send(update_message.encode('utf-8'))
            
            response = sock.recv(1024).decode('utf-8')
            print(f"Degree update response from seed {seed_addr}: {response}")
            
            sock.close()
        except:
            print(f"Failed to update degree to seed {seed_addr}")


# This function is used to connect to seed and send our IP address and port info to seed and then receives a list of peers connected to that seed separated by comma
# After finding union it calls join_atmost_4_peers to connect to atmost peers
def connect_seeds():
    all_peer_lists = []
    for i in range(0, len(connect_seed_addr)):
        try:
            sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            seed_addr = connect_seed_addr[i].split(":")
            ADDRESS = (str(seed_addr[0]), int(seed_addr[1]))
            sock.connect(ADDRESS)
            MY_ADDRESS = str(MY_IP)+":"+str(PORT)
            
            # Send address with initial degree 0
            message = f"{MY_ADDRESS}:0"
            sock.send(message.encode('utf-8'))
            
            message = sock.recv(10240).decode('utf-8')
            print(f"Peer List received from seed {i+1} :- ", message)
            all_peer_lists.append(message)
            sock.close()
        except:
            print(f"Seed Connection Error for seed {i+1}")

    # Combine all received peer lists
    combined_peer_list = ";".join(all_peer_lists)
    
    # Now perform the union operation on the combined list
    complete_peer_list = union_peer_lists(combined_peer_list)
    print("Peer List after processing all seeds:- ", complete_peer_list)
    
    for peer in complete_peer_list:
        write_output_to_file(peer)

    # Power Law function to calculate degree and connect to relevant peers
    newDegree = join_atmost_4_peers(complete_peer_list)

    print("The new degree of the peer is : ", newDegree)
    
    # After joining peers, update the degree
    update_degree_to_seeds(newDegree)



    

# This function is used to register the peer to (floor(n / 2) + 1) random seeds
def register_with_k_seeds():   # where k = floor(n / 2) + 1
    global seeds_addr
    seeds_addr = list(seeds_addr)
    seed_nodes_index = generate_k_random_numbers_in_range(0, n - 1, n // 2 + 1)
    seed_nodes_index = list(seed_nodes_index)
    for i in seed_nodes_index:
        connect_seed_addr.append(seeds_addr[i])
    connect_seeds()

# This function takes address of peer which is down. Generate dead node message and send it to all connected seeds
def report_dead(peer):
    dead_message = f"Dead Node:{peer}:{timestamp()}:{MY_IP}"
    print(dead_message)
    write_output_to_file(dead_message)
    for seed in connect_seed_addr:        
        try:
            sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            seed_address = seed.split(":")
            ADDRESS = (str(seed_address[0]), int(seed_address[1]))
            sock.connect(ADDRESS)
            sock.send(dead_message.encode('utf-8'))
            sock.close()
        except:
            print("Seed Down ", seed)

# This function generates liveness request and it to all connected peers at interval of 13 sec
# If three consecutive replies are not received then call report_dead()
def liveness_testing():    
    while True:
        liveness_request = "Liveness Request:" + str(timestamp()) + ":" + str(MY_IP) + ":" + str(PORT)
        print("Liveness Request sent from :- ", liveness_request)
        for peer in peers_connected:
            try:                
                sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                peer_addr = peer.address.split(":")
                ADDRESS = (str(peer_addr[0]), int(peer_addr[1]))
                sock.connect(ADDRESS)
                sock.send(liveness_request.encode('utf-8'))
                print(sock.recv(1024).decode('utf-8'))
                sock.close()  
                peer.i = 0           # If it is able to send liveness req and get reply then start from 0 again so that we check for 3 consecutive failure
            except:                     # This happens when connection fails so count for 3 consecutive failures for given peer
                peer.i = peer.i + 1   
                if(peer.i == 3):                # If three failures then report this peer as dead node and remove from connected peer list
                    report_dead(peer.address)
                    peers_connected.remove(peer)
        time.sleep(10)

# Forward received gossip message to connected peers if its hash is not in Message List to avoid flooding/looping 
def forward_gossip_message(received_message):
    hash = hash_of_message(received_message) 
    if hash in MessageList:        # If hash of received message is already in Message List then dont forward
        pass
    else:
        MessageList.append(str(hash))   # If it is received for 1st time then append it to its ML list
        print(received_message)
        write_output_to_file(received_message)
        for peer in peers_connected:     # Forward gossip message to all connected peers
            try:
                sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                peer_addr = peer.address.split(":")
                ADDRESS = (str(peer_addr[0]), int(peer_addr[1]))
                sock.connect(ADDRESS)
                sock.send(received_message.encode('utf-8'))
                sock.close()
            except:
                continue

# Generate gossip message and send it to connected peers
def generate_send_gossip_message(i):
    gossip_message = str(timestamp()) + ":" + str(MY_IP) + ":" + str(PORT) + ":" + "GOSSIP" + str(i+1) 
    MessageList.append(str(hash_of_message(gossip_message)))  # Add hash of generated gossip to ML list
    for peer in peers_connected:
        try:
            sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            peer_addr = peer.address.split(":")
            ADDRESS = (str(peer_addr[0]), int(peer_addr[1]))
            sock.connect(ADDRESS)
            sock.send(gossip_message.encode('utf-8'))       
            sock.close() 
        except:
            print("Peer Down ", peer.address)

# Generate 10 gossip messages at an interval of 5 sec each
def gossip():
    for i in range(10):
        generate_send_gossip_message(i)
        time.sleep(5)

# create_workers(), work(), create_jobs() : Referred from internet
# Source : https://github.com/attreyabhatt/Reverse-Shell/blob/master/Multi_Client%20(%20ReverseShell%20v2)/server.py
# Create Worker Threads
def create_workers():
   for _ in range(no_of_threads):
       thread = threading.Thread(target = work)
       thread.daemon = True
       thread.start()

# Do next job that is in the queue (handle connections, liveness testing, gossip)
def work():
   while True:
       x = queue.get()
       if x == 1:
           create_socket()
           bind_socket()
           begin()
       elif x == 2:
           liveness_testing()
       elif x == 3:
           gossip() 
       queue.task_done()

# Create jobs in queue
def create_jobs():
    for i in job_no:
        queue.put(i)
    queue.join()

read_addr_of_seeds()
n = total_available_seeds()
print("Total Available Seeds : ", n)
register_with_k_seeds()                        # Where k = floor(n / 2) + 1

create_workers()
create_jobs()