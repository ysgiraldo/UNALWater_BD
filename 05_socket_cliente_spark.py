import socket

HOST = '127.0.0.1'  # Dirección IP del servidor
PORT = 65432        # Puerto de escucha

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect((HOST, PORT))
    with open('datos.parquet', 'wb') as f:
        while True:
            data = s.recv(1024)
            if not data:
                break
            f.write(data)
