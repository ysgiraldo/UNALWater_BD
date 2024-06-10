import socket

HOST = '127.0.0.1'  # Dirección IP del servidor
PORT = 65432        # Puerto de escucha

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((HOST, PORT))
    s.listen()
    conn, addr = s.accept()
    with conn:
        print('Conexión establecida desde', addr)
        with open('datos.parquet', 'rb') as f:
            while True:
                data = f.read(1024)
                if not data:
                    break
                conn.sendall(data)
