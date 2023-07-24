class Mensagem:
    
    #Inicializa a classe mensagem com seus atributos
    def __init__(self, tipo, key, value, ts, client_ip, client_port):
        self.tipo = tipo
        self.key = key
        self.value = value
        #ts é timestamp
        self.ts = ts
        #Usados para enviar os IPs e portas dos servidores não líderes ao líder ou enviar o IP e porta do cliente ao líder quando um PUT é encaminhado
        self.client_ip = client_ip
        self.client_port = client_port