class Mensagem:
    
    #Inicializa a classe mensagem com seus atributos
    def __init__(self, tipo, key, value, timestamp):
        self.tipo = tipo
        self.key = key
        self.value = value
        self.timestamp = timestamp