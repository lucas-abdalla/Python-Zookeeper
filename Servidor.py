import socket
import threading
import pickle
from Mensagem import Mensagem
import time

class Servidor:
    
    #Cria o socket do servidor
    s = socket.socket()
    #Cria hashtable do servidor
    hash_table = {}
    #O default é o servidor não ser líder
    leader = False
    
    #Inicializa o servidor com IP e porta lidos do teclado
    def __init__(self, IP_self, port_self, IP_leader, port_leader):
        Servidor.IP_self = IP_self
        Servidor.port_self = port_self
        Servidor.IP_leader = IP_leader
        Servidor.port_leader = port_leader
        #Se IP e porta próprios forem os mesmos do IP e porta do líder significa que esse servidor é líder
        if (IP_self == IP_leader and port_self == port_leader):
            Servidor.leader = True
            #Cria uma lista dos outros servidores não líderes
            Servidor.server_list = []
        #Se esse servidor não é líder, envia uma requisição para se conectar com o líder
        else:
            #Abre socket e se conecta com o líder
            d = socket.socket()
            d.connect((IP_leader, port_leader))
            #Instancia uma mensagem de conexão com o líder e envia o próprio IP e porta para o líder
            msg = Mensagem("SERVER_CONNECT", None, None, None, IP_self, port_self)
            pickled_msg = pickle.dumps(msg)
            d.sendall(pickled_msg)
            #Fecha conexão
            d.close()
        #Vincula o servidor ao IP e porta indicados
        Servidor.s.bind((IP_self, port_self))
        #Inicia a escuta do servidor
        Servidor.start_server()

    #Função que cuida das requisições. É chamada por diferentes threads para cada cliente conectado
    def handle_client(c, addr):
        #Captura problemas de desconexão e outros erros que possam ocorrer
        try:
            #Recebe e decodifica a mensagem de requisição
            pickled_msg = c.recv(4096)
            msg = pickle.loads(pickled_msg)
            #Se for um put
            if msg.tipo == "PUT":
                #Se for líder, lida com a requisição ele mesmo
                if Servidor.leader:
                    #Se esse atributo é None, a mensgaem veio direto do cliente. Usa as informações de c para printar
                    if msg.client_ip is None:
                        c_ip, c_port = c.getpeername()
                        print("Cliente %s:%s PUT key: %s value: %s" % (str(c_ip), str(c_port), str(msg.key), str(msg.value)))
                    #Senão, a mensagem foi encaminhada de outro servidor. Usa as informações do cliente enviadas na mensagem para printar
                    else:
                        print("Cliente %s:%s PUT key: %s value: %s" % (str(msg.client_ip), str(msg.client_port), str(msg.key), str(msg.value)))
                    Servidor.put(msg, c, addr)
                #Se não for líder, encaminha a requisição ao líder
                else:
                    #Cria coskcet e conecta com o líder
                    d = socket.socket()
                    d.connect((Servidor.IP_leader, Servidor.port_leader))
                    #Obtém IP e porta do cliente
                    c_ip, c_port = c.getpeername()
                    #Coloca na mensagem o IP e porta do cliente para que possa ser enviado ao servidor líder
                    msg.client_ip = c_ip
                    msg.client_port = c_port
                    #Codifica e manda a mensagem
                    pickled_msg = pickle.dumps(msg)
                    d.sendall(pickled_msg)
                    #Faz o print de encaminhamento
                    print("Encaminhando PUT key: %s value: %s" % (str(msg.key), str(msg.value)))
                    #Recebe e decodifica a resposta
                    pickled_ans = d.recv(4096)
                    ans = pickle.loads(pickled_ans)
                    #Se for um PUT_OK, encaminha o PUT_OK ao cliente
                    if (ans.tipo == "PUT_OK"):
                        c.sendall(pickled_ans)
            #Se for requisição GET, chama a função GET
            elif msg.tipo == "GET":
                Servidor.get(msg, c, addr)
            #Se for uma REPLICATION, chama replicate
            elif msg.tipo == "REPLICATION":
                Servidor.replicate(msg, c, addr)
            #Se for um server não líder tentando se conectar, chama server_connect
            elif msg.tipo == "SERVER_CONNECT":
                Servidor.server_connect(msg, c, addr)
            #Fecha conexões
            c.close()
            addr.close()
        except Exception as e:
            pass

    #Função PUT do servidor
    def put(msg, c, addr):
        #Obtém timestamp em Unix time
        ts = int(time.time())
        #Salva a key, value e timestamp na hashtable do servidor
        Servidor.hash_table[msg.key] = {"value": msg.value, "timestamp": ts}
        #Instancia e codifica a mensagem de replicação
        rep = Mensagem("REPLICATION", msg.key, msg.value, ts, None, None)
        pickled_rep = pickle.dumps(rep)
        #Percorre a lista de servidores conectados ao líder
        for server in Servidor.server_list:
            #Abre o socket, conecta e envia replicação ao servidor
            d = socket.socket()
            d.connect(server)
            d.sendall(pickled_rep)
            #Recebe e decodifica resposta da replicação
            pickled_rep_ans = d.recv(4096)
            rep_ans = pickle.loads(pickled_rep_ans)
            #Trava até receber o REPLICATION_OK
            while True:
                if rep_ans.tipo == "REPLICATION_OK":
                    #Fecha conexão
                    d.close()
                    break
        #Instancia, codifica e envia o PUT_OK ao cliente
        ans = Mensagem("PUT_OK", None, None, ts, None, None)
        pickled_ans = pickle.dumps(ans)
        c.sendall(pickled_ans)
        #Se esse atributo é None, significa que o cliente enviou diretamente ao líder, então printa as informações do cliente
        if msg.client_ip is None:
            c_ip, c_port = c.getpeername()
            print("Enviando PUT_OK ao Cliente %s:%s da key: %s ts: %d" % (str(c_ip), str(c_port), str(msg.key), ts))
        #Senão, a mensagem veio encaminhada de outro servidor. Usa o IP e porta enviado na mensagem para printar
        else:
            print("Enviando PUT_OK ao Cliente %s:%s da key: %s ts: %d" % (str(msg.client_ip), str(msg.client_port), str(msg.key), ts))

    #Função GET do servidor
    def get(msg, c, addr):
        #Obtém IP e porta do cliente
        c_ip, c_port = c.getpeername()
        #Se a key está na hashtable do servidor
        if msg.key in Servidor.hash_table:
            #Pega value e timestamp da key
            value = Servidor.hash_table[msg.key]["value"]
            ts = Servidor.hash_table[msg.key]["timestamp"]
            #Verifica se o timestamp do servidor é mais recente que o do cliente
            if ts >= msg.ts:
                #Instancia a mensagem com a key solicitada e printa as informações
                ans = Mensagem("GET", msg.key, value, ts, None, None)
                print("Cliente %s:%s GET key: %s ts: %d. Meu ts é %d, portanto devolvendo %s" % (str(c_ip), str(c_port), str(msg.key), msg.ts, ts, str(value)))
            #Se não é mais recente, é um erro
            else:
                #Instancia mensagem de erro e printa as informações especificadas
                ans = Mensagem("TRY_OTHER_SERVER_OR_LATER", None, None, None, None, None)
                print("Cliente %s:%s GET key: %s ts: %d. Meu ts é %d, portanto devolvendo TRY_OTHER_SERVER_OR_LATER" % (str(c_ip), str(c_port), str(msg.key), msg.ts, ts))
            #Codifica e envia a mensagem
            pickled_ans = pickle.dumps(ans)
            c.sendall(pickled_ans)
        #Se essa key não está no servidor e o timestamp dela no cliente é 0, significa que essa chave ainda não existe
        elif msg.ts == 0:
            #Seta value para None, como especificado, e timestamp para 0, pois essa chave não existe
            value = None
            ts = 0
            #Printa mensagem com o valor None
            print("Cliente %s:%s GET key: %s ts: %d. Meu ts é %d, portanto devolvendo %s" % (str(c_ip), str(c_port), str(msg.key), msg.ts, ts, str(value)))
            #Instancia resposta com None, codifica e a envia
            ans = Mensagem("GET", msg.key, value, ts, None, None)
            pickled_ans = pickle.dumps(ans)
            c.sendall(pickled_ans)
        #Se a key não existe no servidor mas o timestamp dela no cliente não é 0, significa que ainda não foi replicada nesse servidor
        else:
            #Seta o timestamp em 0
            ts = 0
            #Instancia a mensagem de erro
            ans = Mensagem("TRY_OTHER_SERVER_OR_LATER", None, None, None, None, None)
            #Faz o print especifica, dizendo que enviou erro ao cliente
            print("Cliente %s:%s GET key: %s ts: %d. Meu ts é %d, portanto devolvendo TRY_OTHER_SERVER_OR_LATER" % (str(c_ip), str(c_port), str(msg.key), msg.ts, ts))
            #Codifica e envia mensagem ao cliente
            pickled_ans = pickle.dumps(ans)
            c.sendall(pickled_ans)

    #Função REPLICATION do servidor
    def replicate(msg, c, addr):
        #Salva a key, value e timestamp recebidas do líder
        Servidor.hash_table[msg.key] = {"value": msg.value, "timestamp": msg.ts}
        #Instancia e codifica a mensagem de REPLICATION_OK
        rep_ans = Mensagem("REPLICATION_OK", None, None, None, None, None)
        pickled_rep_ans = pickle.dumps(rep_ans)
        #Faz o print de REPLICATION
        print("REPLICATION key: %s value: %s ts: %d" % (str(msg.key), str(msg.value), msg.ts))
        #Envia o REPLICATION_OK ao líder
        c.sendall(pickled_rep_ans)

    #O servidor líder salva os outros servidores na sua lista
    def server_connect(msg, c, addr):
        Servidor.server_list.append((msg.client_ip, msg.client_port))

    #Função que inicia o servidor
    def start_server():
        #Começa a escutar por clientes
        Servidor.s.listen(1)
        #Sempre escutando
        while True:
            #Aceita conexão
            c, addr = Servidor.s.accept()
            #Envia novas conexões para threads para poder cuidar de múltiplos clientes ao mesmo tempo
            c_thread = threading.Thread(target=Servidor.handle_client, args=(c, addr))
            c_thread.start()

#Captura do teclado IP e porta do servidor e do servidor líder
IP_self = input()
port_self = int(input())
IP_leader = input()
port_leader = int(input())
#Instancia o servidor
svr = Servidor(IP_self, port_self, IP_leader, port_leader)