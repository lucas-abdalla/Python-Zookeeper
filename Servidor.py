import socket
import threading
import pickle
import Mensagem

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
        Servidor.s.bind((IP_self, port_leader))
        Servidor.start_server()

    #Função que cuida das requisições dos peers. É chamada por diferentes threads para cada peer conectado
    def handle_client(c, addr):
        #Captura problemas de desconexão e outros erros que possam ocorrer
        try:
            #Recebe dados do peer em bytes
            data = c.recv(2097152)
            #Verifica se os dados são do tipo pickle verificando se a primeira posição é 0x80, que é o início da codificação pickle.
            #Se os dados são do tipo pickle significa que é uma requisição do tipo join
            if data[0] == 0x80:
                Servidor.join(c, addr, data)
            else:
                #Se não é pickle, caso tenha recebido uma string UPDATE, é uma requisição UPDATE
                aux = data.decode("utf-8")
                if aux == "UPDATE":
                    Servidor.update(c, addr)
                #Se não era uma requisição update era uma requisição SEARCH. Os bytes codificados são enviados à função SEARCH e são tratados nela
                else:
                    Servidor.search(c, addr, data)
        #Caso capture um erro não há prints para manter no console somente os prints especificados
        except Exception as e:
            pass

    #Função que inicia o servidor
    def start_server():
        #Começa a escutar por peers
        Servidor.s.listen(5)
        #Sempre escutando
        while True:
            #Aceita conexão
            c, addr = Servidor.s.accept()
            #Envia novas conexões para threads para poder cuidar de múltiplos peers ao mesmo tempo
            c_thread = threading.Thread(target=Servidor.handle_client, args=(c, addr))
            c_thread.start()

#Captura do teclado IP e porta do servidor
IP_self = input()
port_self = int(input())
IP_leader = input()
port_leader = int(input())
#Instancia o servidor
svr = Servidor(IP_self, port_self, IP_leader, port_leader)