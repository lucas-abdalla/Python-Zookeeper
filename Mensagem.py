class Mensagem:
    
    #Inicializa a classe mensagem com seus atributos
    def __init__(self, tipo, key, value, ts, client_ip, client_port):
        self.tipo = tipo
        self.key = key
        self.value = value
        self.ts = ts
        self.client_ip = client_ip
        self.client_port = client_port