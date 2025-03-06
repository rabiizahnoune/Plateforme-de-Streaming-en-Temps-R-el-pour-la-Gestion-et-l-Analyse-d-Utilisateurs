import requests
import json
import time 
from kafka import KafkaProducer
from requests.exceptions import RequestException


#configiration du producer kafka
producer = KafkaProducer(
    bootstrap_servers = 'localhost:9092',
    value_serializer =lambda v: json.dumps(v).encode('utf-8')

)


#url de l'API random user pour recuperer 10 utilisateurs
API_URL = "https://randomuser.me/api/?results=10"


#nom du topic kafka
TOPIC = 'user-stream'


#frequence d'envoi (en secondes)
FREQUENCY = 10

#delai de retry en cas d'echec (en secondes)
RETRY_DELAY = 60

def fetch_users():
    """recupere un batch de 10 utilisateurs depuis l'api random user."""
    try:
        #faire l appel get a l api
        response = requests.get(API_URL,timeout=10)
        #verifier le code HTTP
        response.raise_for_status()  # leve une exception si code #200
        #recuperer le json et retourner le champ 'results'
        data = response.json()
        return data['results']
    except RequestException as e:
        #en casa d'erreur(timout,connexion)
        print(f'Erreur lors de l appel API: {e}')
        return None
    
def send_to_kafka(users):
    """envoi un batch d utilisateurs vers kafka."""
    if users:
        try:
            #envoyer le batch complet (liste 10 utilisateurs) comme un seul message 
            producer.send(TOPIC,users)
            producer.flush() #assurer que le message est envoye immediatement 
            print(f"envoye {len(users)} utilisateurs a kafka dans le topic '{TOPIC}")
        except Exception as e:
            print(f"erreur lors de l envoi a kafka : {e}")
    else:
        print("aucun utilisateurs pour recuperer et envoyer les donnees")


def main():
    """boucle principale pour recuperer et envoyer les donees."""
    while True:
        #recuperer un batch d'utilisateurs
        users = fetch_users()

        if users is not None:
            #si succes, envoyer a kafka et attendre la frequence normale
            send_to_kafka(users)
            time.sleep(FREQUENCY)
        else:
            print(f"attente de {RETRY_DELAY} secondes avant de ressayer")
            time.sleep(RETRY_DELAY)


if __name__ == '__main__':
    print('demarrage du producteur python...')
    main()


