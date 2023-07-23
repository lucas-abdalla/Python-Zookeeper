import socket
import threading
import pickle
import Mensagem
import datetime

class Servidor:
    
    #Cria o socket do servidor
    s = socket.socket()
    #Cria hashtable do servidor
    hash_table = {}
    leader = False
    
    #Inicializa o servidor com IP e porta lidos do teclado
    def __init__(self, IP_self, port_self, IP_leader, port_leader):
        Servidor.IP_self = IP_self
        Servidor.port_self = port_self
        Servidor.IP_leader = IP_leader
        Servidor.port_leader = port_leader
        if (IP_self == IP_leader and port_self == port_leader):
            Servidor.leader = True
            Servidor.server_list = []
        else:
            d = socket.socket()
            d.connect((IP_leader, port_leader))
            msg = Mensagem("SERVER_CONNECT", None, None, None, None, None)
            pickled_msg = pickle.dumps(msg)
            d.sendall(pickled_msg)
            d.close()
        Servidor.s.bind((IP_self, port_leader))
        Servidor.start_server()

    #Função que cuida das requisições. É chamada por diferentes threads para cada cliente conectado
    def handle_client(c, addr):
        #Captura problemas de desconexão e outros erros que possam ocorrer
        try:
            pickled_msg = c.recv(4096)
            msg = pickle.loads(pickled_msg)
            if msg.tipo == "PUT":
                if Servidor.leader:
                    c_ip, c_port = c.getpeername()
                    print("Cliente %s:%s PUT key: %s value: %s" % (str(c_ip), str(c_port), str(msg.key), str(msg.value)))
                    Servidor.put(msg, c, addr)
                else:
                    d = socket.socket()
                    d.connect((Servidor.IP_leader, Servidor.port_leader))
                    c_ip, c_port = c.getpeername()
                    msg.client_ip = c_ip
                    msg.client_port = c_port
                    pickled_msg = pickle.dumps(msg)
                    d.sendall(pickled_msg)
                    #Servidor.s.connect((Servidor.IP_leader, Servidor.port_leader))
                    #Servidor.s.sendall(pickled_msg)
                    #Servidor.s.close()
                    print("Encaminhando PUT key: %s value: %s" % (str(msg.key), str(msg.value)))
                    pickled_ans = d.recv(4096)
                    ans = pickle.loads(pickled_ans)
                    if (ans.tipo == "PUT_OK"):
                        c.sendall(pickled_ans)
            elif msg.tipo == "GET":
                Servidor.get(msg, c, addr)
            elif msg.tipo == "REPLICATION":
                Servidor.replicate(msg, c, addr)
            elif msg.tipo == "SERVER_CONNECT":
                Servidor.server_connect(msg, c, addr)
        except Exception as e:
            pass

    def put(msg, c, addr):
        dt = datetime.now()
        ts = dt.timestamp()
        Servidor.hash_table[msg.key] = {"value": msg.value, "timestamp": ts}
        rep = Mensagem("REPLICATION", msg.key, msg.value, ts, None, None)
        pickled_rep = pickle.dumps(rep)
        for server in Servidor.server_list:
            d = socket.socket()
            d.connect(server)
            d.sendall(pickled_rep)
            pickled_rep_ans = d.recv(4096)
            rep_ans = pickle.loads(pickled_rep_ans)
            while True:
                if rep_ans.tipo == "REPLICATION_OK":
                    d.close()
                    break
        ans = Mensagem("PUT_OK", None, None, ts, None, None)
        pickled_ans = pickle.dumps(ans)
        c.sendall(pickled_ans)
        if (msg.client_ip == None):
            c_ip, c_port = c.getpeername()
            print("Enviando PUT_OK ao Cliente %s:%s da key: %s ts: %d" % (str(c_ip), str(c_port), str(msg.key), msg.ts))
        else:
            print("Enviando PUT_OK ao Cliente %s:%s da key: %s ts: %d" % (str(msg.client_ip), str(msg.client_port), str(msg.key), msg.ts))
        #c_ip, c_port = c.getpeername()
        #print("Cliente %s:%s PUT key: %s value: %s" % (str(c_ip), str(c_port), str(msg.key), str(msg.value)))

    def get(msg, c, addr):
        pass

    def replicate(msg, c, addr):
        pass

    def server_connect(msg, c, addr):
        c_ip, c_port = c.getpeername()
        Servidor.server_list.append((c_ip, c_port))

    #Função que inicia o servidor
    def start_server():
        #Começa a escutar por clientes
        Servidor.s.listen(5)
        #Sempre escutando
        while True:
            #Aceita conexão
            c, addr = Servidor.s.accept()
            #Envia novas conexões para threads para poder cuidar de múltiplos clientes ao mesmo tempo
            c_thread = threading.Thread(target=Servidor.handle_client, args=(c, addr))
            c_thread.start()

#Captura do teclado IP e porta do servidor
IP_self = input()
port_self = int(input())
IP_leader = input()
port_leader = int(input())
#Instancia o servidor
svr = Servidor(IP_self, port_self, IP_leader, port_leader)