import socket
import threading

MY_IP = "0.0.0.0"                                               # IP for listening
PORT = int(input())                                             # Port No. for listening               
peer_list = {}  # Use this at the beginning of your seed node script       

# Write the outputs to the file
def write_output_to_file(output):
    try:
        file = open("outputseed.txt", "a")  
        file.write(output + "\n") 
    except:
        print("Write Failed")
    finally:
        file.close()

# Create socket to connect 2 computers
def create_socket():
    try:
        global socket
        socket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        print("Socket Created\n")
    except:
        print("Socket Creation Error")

# Bind socket
def bind_socket():
    try:
        global socket
        ADDRESS = (MY_IP, PORT)
        socket.bind(ADDRESS)
        print("Socket Bounded")
    except:
        print("Socket Binding Error")
        bind_socket()

# It take list of connected peer and convert it to comma separated string to send it through socket
def list_to_string(peer_list):  
    PeerList = ","  
    for i in peer_list:  
        PeerList += i + ","    
    return PeerList  

# Take the dead node message, find address of dead node from it and remove it from peer list if present
def remove_dead_node(message):
    dead_node, informer_node = message.split(":")[1:]
    
    if dead_node in peer_list:
        del peer_list[dead_node]
        print(f"Removed dead node: {dead_node}")
        write_output_to_file(f"{MY_IP}:{PORT} - Removed dead node: {dead_node}")
    else:
        print(f"Dead node {dead_node} not found in peer list")
    
    if informer_node in peer_list:
        peer_list[informer_node] = max(0, peer_list[informer_node] - 1)
        print(f"Reduced degree of informer node {informer_node} to {peer_list[informer_node]}")
    else:
        print(f"Informer node {informer_node} not found in peer list")



def update_peer_degree(peer_address, new_degree):
    if peer_address in peer_list:
        peer_list[peer_address] = new_degree
        print(f"Updated degree for {peer_address} to {new_degree}")
    else:
        print(f"Peer {peer_address} not found in the list")

def dict_to_string(peer_dict):
    return ";".join([f"{addr}:{degree}" for addr, degree in peer_dict.items()])

def incrementDegreeOfPeer(peer_address):
    if peer_address in peer_list:
        peer_list[peer_address] += 1
        print(f"Incremented degree for {peer_address} to {peer_list[peer_address]}")
        return f"Degree incremented to {peer_list[peer_address]}"
    else:
        print(f"Peer {peer_address} not found in the list")
        return "Peer not found"



# To handle different peers in threads.It receives peer address from peer and add it to its peer list
# If it receives dead node message then pass it remove_dead_node()
def handle_peer(conn, addr):
    while True:
        try:
            message = conn.recv(1024).decode('utf-8')      
            if message:
                if message.startswith("INCREMENT:"):
                    # Handle degree increment
                    _, peer_address, peer_port = message.split(":")
                    print("Received Increment request from :- ", peer_address+":"+peer_port)
                    response = incrementDegreeOfPeer(peer_address+":"+peer_port)
                    conn.send(response.encode('utf-8'))
                    write_output_to_file(f"{MY_IP}:{PORT} - Degree updated successfully - {peer_address}:{peer_port}")

                elif message.startswith("UPDATE:"):
                    # Handle degree update
                    print("Degree Update Request Received :- ", message)
                    _, peer_address, peer_port,  new_degree = message.split(":")
                    update_peer_degree(peer_address+peer_port, int(new_degree))
                    conn.send(f"Degree updated successfully - {peer_address}:{peer_port}".encode('utf-8'))
                    write_output_to_file(f"{MY_IP}:{PORT} - Degree updated successfully - {peer_address}:{peer_port}")

                elif "Dead Node" in message[0:9]:
                    remove_dead_node(message)

                else:
                    message = message.split(":")
                    peer_address = str(addr[0])+":"+str(message[1])
                    peer_degree = int(message[2])  # This will be 0 initially
                    peer_list[peer_address] = peer_degree
                    output = f"{MY_IP}:{PORT} - Received Connection from {peer_address}"
                    print(output)
                    write_output_to_file(output)
                    PeerList = dict_to_string(peer_list)
                    conn.send(PeerList.encode('utf-8'))

                    print("Peer List: ", peer_list)
        except:
            break
    conn.close()


# To listen at a particular port and create thread for each peer  
def begin():
    socket.listen(5)
    print("Seed is Listening")
    while True:
        conn, addr = socket.accept()
        socket.setblocking(1)
        thread = threading.Thread(target=handle_peer, args=(conn,addr))
        thread.start()
        
create_socket()
bind_socket()
begin()