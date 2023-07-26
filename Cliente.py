import socket
import pickle
from Mensagem import Mensagem
import random

class Cliente:
    
    #Cria hashtable do cliente
    hash_table = {}
    hash_table["star"] = {"value": "wars", "timestamp": 10000}
    #Inicializa o cliente com as informações capturadas do teclado
    def __init__(self, IP_list, port_list):
        self.IP = IP_list
        self.port = port_list

    #Função PUT do cliente
    def put(self):
        #Cria socket 
        s = socket.socket()
        #Seta tipo da mensagem e recebe key e value do teclado
        tipo = "PUT"
        key = input()
        value = input()
        #Instancia a classe Mensagem
        msg = Mensagem(tipo, key, value, None, None, None)
        #Gera um número aleatório entre 0 e 2 e usa esse número para selecionar um dos servidores
        server = random.randint(0, 2)
        #Conecta ao servidor usando o número aleatório
        s.connect((self.IP[server], self.port[server]))
        #Codifica a instancia da classe mensagem para enviá-la pela rede
        pickled_msg = pickle.dumps(msg)
        #Envia a classe mensagem
        s.sendall(pickled_msg)
        #Recebe a resposta do PUT
        pickled_ans = s.recv(4096)
        #Decodifica a resposta recebida
        ans = pickle.loads(pickled_ans)
        #Se foi um PUT_OK
        if ans.tipo == "PUT_OK":
            #Salva na hash_table do cliente, faz o print especificado e fecha a conexão
            self.hash_table[key] = {"value": value, "timestamp": ans.ts}
            print("PUT_OK key: %s value %s timestamp %d realizada no servidor %s:%d" % (str(key), str(value), ans.ts, self.IP[server], self.port[server]))
            s.close()

    #Função GET do cliente
    def get(self):
        #Cria socket
        s = socket.socket()
        #Seta tipo da mensagem e recebe key do teclado
        tipo = "GET"
        key = input()
        #Se a key está no hash_table do cliente, pega a sua timestamp atual
        if key in self.hash_table:
            ts = self.hash_table[key]["timestamp"]
        #Senão seta o timestamp em 0
        else:
            ts = 0
        #Instancia a mensagem
        msg = Mensagem(tipo, key, None, ts, None, None)
        #Gera o número aleatório e o usa para conectar a um dos servidores
        server = random.randint(0, 2)
        s.connect((self.IP[server], self.port[server]))
        #Codifica e envia a mensagem ao servidor
        pickled_msg = pickle.dumps(msg)
        s.sendall(pickled_msg)
        #Recebe e decodifica a resposta do servidor
        pickled_ans = s.recv(4096)
        ans = pickle.loads(pickled_ans)
        #Se não for um erro e o servidor possuir a chave
        if ans.tipo != "TRY_OTHER_SERVER_OR_LATER" and ans.value is not None:
            #Salva na hashtable do cliente e faz o print especificado
            self.hash_table[key] = {"value": ans.value, "timestamp": ans.ts}
            print("GET key: %s value %s obtido do servidor %s:%d, meu timestamp %d e do servidor %d" % (str(key), str(ans.value), self.IP[server], self.port[server], ts, ans.ts))
        #Se for um erro, faz print do erro
        elif ans.tipo == "TRY_OTHER_SERVER_OR_LATER":
            print(ans.tipo)
        #Se for uma chave ainda inexistente, faz o print especificado e não salva na hashtable do cliente
        elif ans.value is None:
            print("GET key: %s value %s obtido do servidor %s:%d, meu timestamp %d e do servidor %d" % (str(key), str(ans.value), self.IP[server], self.port[server], ts, ans.ts))
        #Fecha conexão
        s.close()

#Função simples que printa o menu interativo no console
def printMenu():
        print("Escolha uma das opções digitando os números indicados:\n")
        print("1: Inicializar cliente (INIT)")
        print("2: Enviar requisição PUT (PUT)")
        print("3: Enviar requisição GET (GET)")

#Funcionamento lógico do menu interativo
def menu(c):
    #IPs e portas dos servidores serão salvos em arrays
    IP_list = []
    port_list = []
    #Printa menu
    printMenu()
    #Obtem ocção selecionada
    option = int(input())
    #Chama função INIT
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