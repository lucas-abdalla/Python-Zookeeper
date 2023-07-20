import socket
import pickle
import threading
import datetime
import Mensagem
import random

class Cliente:

    #Cria um socket para a classe Cliente
    s = socket.socket()
    #Cria hashtable do cliente
    hash_table = {}

    #Inicializa o cliente com as informações capturadas do teclado
    def __init__(self, IP_list, port_list):
        self.IP = IP_list
        self.port = port_list
        dt = datetime.now()
        self.timestamp = dt.timestamp()

    def put(self):
        tipo = "PUT"
        key = input()
        value = input()
        msg = Mensagem(tipo, key, value, None)
        server = random.randint(0, 2)
        self.s.connect((self.IP[server], self.port[server]))
        pickled_msg = pickle.dumps(msg)
        self.s.sendall(pickled_msg)
        pickled_ans = self.s.recv(4096)
        ans = pickle.loads(pickled_ans)
        if ans.tipo == "PUT_OK":
             self.hash_table[key] = {"value": value, "timestamp": ans.ts}
             print("PUT_OK key: %s value %s timestamp %d realizada no servidor %s:%d" % (key, value, ans.ts, self.IP[server], self.port[server]))
             self.s.close()

    def get(self):
         pass

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
    #Chama função SEARCH
    elif option == 2:
        c.put()
        menu(c)
    #Chama função DOWNLOAD
    elif option == 3:
        c.download()
        menu(c)

#Cria variável global p para ser instância de cliente após o join e manter mesma instância ao chamar o menu repetidamente
global c
c = None
menu(c)