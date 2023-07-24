import socket
import pickle
import time
#import datetime
from Mensagem import Mensagem
import random

class Cliente:

    #Cria um socket para a classe Cliente
    #s = socket.socket()
    #Cria hashtable do cliente
    hash_table = {}

    #Inicializa o cliente com as informações capturadas do teclado
    def __init__(self, IP_list, port_list):
        self.IP = IP_list
        self.port = port_list
        #dt = datetime.now()
        #self.timestamp = dt.timestamp()
        #timestamp = int(time.time())

    def put(self):
        s = socket.socket()
        tipo = "PUT"
        key = input()
        value = input()
        msg = Mensagem(tipo, key, value, None, None, None)
        server = random.randint(0, 2)
        #self.s.connect((self.IP[server], self.port[server]))
        #if not self.s.getsockname():
        #self.s.connect(("127.0.0.1", 10097))
        s.connect((self.IP[server], self.port[server]))
        pickled_msg = pickle.dumps(msg)
        #self.s.sendall(pickled_msg)
        s.sendall(pickled_msg)
        #pickled_ans = self.s.recv(4096)
        pickled_ans = s.recv(4096)
        ans = pickle.loads(pickled_ans)
        if ans.tipo == "PUT_OK":
            self.hash_table[key] = {"value": value, "timestamp": ans.ts}
            print("PUT_OK key: %s value %s timestamp %d realizada no servidor %s:%d" % (str(key), str(value), ans.ts, self.IP[server], self.port[server]))
            #print("PUT_OK key: %s value %s timestamp %d realizada no servidor %s:%d" % (str(key), str(value), ans.ts, "127.0.0.1", 10097))
            s.close()

    def get(self):
        s = socket.socket()
        tipo = "GET"
        key = input()
        if key in self.hash_table:
            ts = self.hash_table[key]["timestamp"]
        else:
            ts = 0
        msg = Mensagem(tipo, key, None, ts, None, None)
        server = random.randint(0, 2)
        #self.s.connect((self.IP[server], self.port[server]))
        s.connect((self.IP[server], self.port[server]))
        pickled_msg = pickle.dumps(msg)
        #self.s.sendall(pickled_msg)
        s.sendall(pickled_msg)
        #pickled_ans = self.s.recv(4096)
        pickled_ans = s.recv(4096)
        ans = pickle.loads(pickled_ans)
        if ans.tipo != "TRY_OTHER_SERVER_OR_LATER" and ans.value is not None:
            self.hash_table[key] = {"value": ans.value, "timestamp": ans.ts}
            print("GET key: %s value %s obtido do servidor %s:%d, meu timestamp %d e do servidor %d" % (str(key), str(ans.value), self.IP[server], self.port[server], ts, ans.ts))
        elif ans.tipo == "TRY_OTHER_SERVER_OR_LATER":
            print(ans.tipo)
        elif ans.value is None:
            print("GET key: %s value %s obtido do servidor %s:%d, meu timestamp %d e do servidor %d" % (str(key), str(ans.value), self.IP[server], self.port[server], ts, ans.ts))
        s.close()


#Função simples que printa o menu interativo no console
def printMenu():
        print("Escolha uma das opções digitando os números indicados:\n")
        print("1: Inicializar cliente (INIT)")
        print("2: Enviar requisição PUT (PUT)")
        print("3: Enviar requisição GET (GET)")

#Funcionamento lógico do menu interativo
def menu(c):
    IP_list = []
    port_list = []
    printMenu()
    #Obtem ocção selecionada
    option = int(input())
    if option == 1:
        #Obtém IP e endereço dos servidores
        IP_list.append(input())
        port_list.append(int(input()))
        IP_list.append(input())
        port_list.append(int(input()))
        IP_list.append(input())
        port_list.append(int(input()))
        #Inicializa peer
        c = Cliente(IP_list, port_list)
        menu(c)
    #Chama função PUT
    elif option == 2:
        c.put()
        menu(c)
    #Chama função GET
    elif option == 3:
        c.get()
        menu(c)

#Cria variável global p para ser instância de cliente após o join e manter mesma instância ao chamar o menu repetidamente
global c
c = None
menu(c)