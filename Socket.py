import socket
import threading

# Configuración del servidor
server_address = ('localhost', 12345)
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind(server_address)
server_socket.listen(5)

clients = []

def handle_client(client_socket):
    while True:
        try:
            data = client_socket.recv(1024)
            if not data:
                break
            for client in clients:
                if client != client_socket:
                    client.sendall(data)
        except ConnectionResetError:
            break
    client_socket.close()
    clients.remove(client_socket)

def accept_clients():
    while True:
        client_socket, client_address = server_socket.accept()
        clients.append(client_socket)
        print(f"Conexión aceptada de {client_address}")
        client_handler = threading.Thread(target=handle_client, args=(client_socket,))
        client_handler.start()

print("Servidor de socket escuchando en el puerto 12345...")
accept_thread = threading.Thread(target=accept_clients)
accept_thread.start()

#docker exec -it spark /bin/bash en una terminal cmd