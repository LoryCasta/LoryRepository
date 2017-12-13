import socket
import sqlite3
import datetime
import threading
from threading import Lock
import logging
from pathlib import Path


lock = Lock()
# nomi dei file relativi al mio spazio di lavoro
logging.basicConfig(filename='C:/Users/Utente/Desktop/file_logging.out', level=logging.DEBUG)
fiel_name_db = 'C:/Users/Utente/Desktop/database.db'
fiel_name_schema = 'C:/Users/Utente/Desktop/schema.sql'

# realizzo la classe Thread
class ClientThread(threading.Thread):
    def __init__(self, clientAddress, clientsocket):
        threading.Thread.__init__(self)
        self.csocket = clientsocket
        print("Nuova connessione rilevata e aggiunta: ",clientAddress)

    def run(self):

        notifica = client_socket.recv(300)

        print(notifica)
        logging.info('Thread creato e attivato')
        corrispondenze = 0
        for l in lista:
            trovato = notifica.decode().upper().find(l.upper())
            if(trovato > -1):
                corrispondenze = corrispondenze + 1         # cerco quanto corrispondenze delle label trovo nella notifi
                print(l)

        try:
            id = 'NULL'
            if(corrispondenze > 1):
                logging.info('Il thread aspetta di accedere al db')
                print('Inserisco una tupla nel db con più di una corrispondenza')
                lock.acquire()  # Uso i lock per rendere le operazioni di modifica del db atomiche
                database = sqlite3.connect(fiel_name_db)
                cursore = database.cursor()
                logging.info('Thread possiede il db')
                # aggiorno il db
                corpo = notifica.decode()
                params = (corpo, id)
                logging.debug('query effettuata:INSERT INTO notifications VALUES (NULL, ?, ?)""", params')
                cursore.execute("""INSERT INTO notifications VALUES (NULL, ?, ?)""", params)
                data = datetime.datetime.now()
                # Prima di aggiornare la tabella notification_counters prima devo verificare se la tupla relativa a quel
                # cliente in quella data esiste altrimenti la creo
                params = (id, data)
                if (cursore.execute(
                        """SELECT num FROM notification_counters WHERE id_customer == ? AND day == ?""", params) is not None):
                    cursore.execute(
                        """UPDATE notification_counters SET num=num+1 WHERE id_customer == ? AND day == ?""", params)
                    logging.debug('query effettuata: UPDATE notification_counters SET num=num+1 WHERE id_customer == ? AND day == ?""", params')
                else:
                    cursore.execute("""INSERT INTO notification_counters (id_customer,num,day) VALUES (id,0,data)""")
                    logging.debug(
                        'query effettuata: INSERT INTO notification_counters (id_customer,num,day) VALUES (id,0,data)')
                # cursore.commit()
                cursore.close()
                lock.release()
                logging.info('Thread ha finito di modificare il db')
            else:
                print('Inserisco una tupla nel db con una corrispondenza')
                for l in lista:
                    trovato = notifica.decode().upper().find(l.upper())  # cerco una corrispndenza della label nel testo ricevuto
                    if (trovato > -1):
                        logging.info('Il thread aspetta di accedere al db')
                        lock.acquire()  # Uso i lock per rendere le operazioni di modifica del db atomiche
                        notification_label = l.upper()
                        database = sqlite3.connect(fiel_name_db)
                        cursore = database.cursor()
                        logging.info('Thread possiede il db')
                        cursore.execute("""SELECT id FROM customers WHERE notification_label == ?""", [notification_label])
                        logging.debug(
                            'query effettuata:"""SELECT id FROM customers WHERE notification_label == ?""", [notification_label])')
                        id = cursore.fetchone()
                        # aggiorno il db
                        corpo = notifica.decode()
                        params = (corpo, id)
                        cursore.execute("""INSERT INTO notifications VALUES (NULL, ?, ?)""", params)
                        logging.debug(
                            'query effettuata:"""INSERT INTO notifications VALUES (NULL, ?, ?)""", params')
                        data = datetime.datetime.now()
                        # Prima di aggiornare la tabella notification_counters prima devo verificare se la tupla relativa a quel
                        # cliente in quella data esiste altrimenti la creo
                        params = (id, data)
                        if (cursore.execute(
                                """SELECT num FROM notification_counters WHERE id_customer == ? AND day == ?""", params) is not None):
                            cursore.execute(
                                 """UPDATE notification_counters SET num=num+1 WHERE id_customer == ? AND day == ?""", params)
                            logging.debug(
                                'query effettuata:"""UPDATE notification_counters SET num=num+1 WHERE id_customer == ? AND day == ?""", params')
                        else:
                            cursore.execute("""INSERT INTO notification_counters (id_customer,num,day) VALUES (id,0,data)""")
                            logging.debug(
                                'query effettuata:"""INSERT INTO notification_counters (id_customer,num,day) VALUES (id,0,data)"""')
                        # cursore.commit()
                        cursore.close()
                        lock.release()
                        logging.info('Thread ha finito di modificare il db')
        except ValueError:
            logging.error('errore registrato ', ValueError)

# realizzo il database
file_db = Path(fiel_name_db)
if file_db.is_file():
    print("Il database è già stato realizzato")
else:
    database = sqlite3.connect(fiel_name_db)
    cursore = database.cursor()

    print('Creazione dello schema')
    with open(fiel_name_schema, 'rt') as f:
        schema = f.read()
    database.executescript(schema)
    database.close()
# mi salvo in una lista le label presenti nel db

lista = []
database = sqlite3.connect(fiel_name_db)
cursore = database.cursor()
i = 0
for label in database.execute("""SELECT notification_label FROM customers """):
    i = i + 1
cur = database.cursor()
cur.execute("""SELECT notification_label FROM customers """)


for row in cur.fetchmany(i):
    lista.append(row[0])

# mostro la lista
# for element in lista:
#    print(element)

cursore.close()

# __main__
porta = 30123
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server_socket.bind(("", porta))
print("Server partito")
logging.info('Server partito')
server_socket.listen(5)


while True:
    print("Server in attesa di connessioni")
    logging.info('Server in attesa di connessioni')
    server_socket.listen(5)
    client_socket, clientAddress = server_socket.accept()
    # Riceviamo i dati inviati dal client
    newthread = ClientThread(clientAddress, client_socket)
    newthread.start()