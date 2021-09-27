
import requests
import json
from pprint import pprint
from pymongo import MongoClient

def get_vlille():
    url = "https://opendata.lillemetropole.fr/api/records/1.0/search/?dataset=vlille-realtime&q=&rows=3000&facet=libelle&facet=nom&facet=commune&facet=etat&facet=type&facet=etatconnexion"
    response = requests.request("GET", url)
    response_json = json.loads(response.text.encode('utf8'))
    return response_json.get("records", [])

def get_vParis():
    url = "https://opendata.paris.fr/api/records/1.0/search/?dataset=velib-disponibilite-en-temps-reel&q=&rows=-1"
    response = requests.request("GET", url)
    response_json = json.loads(response.text.encode('utf8'))
    return response_json.get("records", [])

def get_vRennes():
    url = "https://data.rennesmetropole.fr/api/records/1.0/search/?dataset=stations_vls&q="
    response = requests.request("GET", url)
    response_json = json.loads(response.text.encode('utf8'))
    return response_json.get("records", [])

def get_vLyon():
    url =  "https://download.data.grandlyon.com/ws/rdata/jcd_jcdecaux.jcdvelov/all.json"
    response = requests.request("GET", url)
    response_json = json.loads(response.text.encode('utf8'))
    return response_json.get("values", [])

vlilles = get_vlille()
vlilles_to_insert = [
    {
        'name': elem.get('fields', {}).get('nom', '').title(),
        'geometry': elem.get('geometry'),
        'size': elem.get('fields', {}).get('nbvelosdispo') + elem.get('fields', {}).get('nbplacesdispo'),
        'source': {
            'dataset': 'Lille',
            'id_ext': elem.get('fields', {}).get('libelle')
        },
        'tpe': elem.get('fields', {}).get('type', '') == 'AVEC TPE'
    }
    for elem in vlilles
]

vParis = get_vParis()
vParis_to_insert = [
    {
        'name': elem.get('fields', {}).get('name', '').title(),
        'geometry': elem.get('geometry'),
        'size': elem.get('fields', {}).get('capacity'),
        'source': {
            'dataset': 'Paris',
            'id_ext': elem.get('fields', {}).get('numdocksavailable')
        },
        'tpe': elem.get('fields', {}).get('type', '') == 'AVEC TPE'
    }
    for elem in vParis
]

vRennes = get_vRennes()
vRennes_to_insert = [
    {
        'name': elem.get('fields', {}).get('nom', '').title(),
        'geometry': elem.get('geometry'),
        'size': elem.get('fields', {}).get('nb_socles'),
        'source': {
            'dataset': 'Rennes',
            'id_ext': elem.get('fields', {}).get('libelle')
        },
        'tpe': elem.get('fields', {}).get('tpe', '') == 'oui'
    }
    for elem in vRennes
]
vLyon = get_vLyon()   
vLyon_to_insert = [
    {
        'name': elem.get('fields', {}).get('nom', '').title(),
        'geometry': elem.get('geometry'),
        'size': elem.get('fields', {}).get('nb_socles'),
        'source': {
            'dataset': 'Lyon',
            'id_ext': elem.get('fields', {}).get('libelle')
        },
        'tpe': elem.get('fields', {}).get('tpe', '') == 'oui'
    }
    for elem in vRennes
]
#'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''' 
vLyon_format = []
for vlib in vLyon:
    vLyon_format.append({
        "name": vlib["name"],
        "city": vlib["commune"],
        "size": vlib["bike_stands"],
        "geo": {"type": "Point", "coordinates": [vlib["lng"], vlib["lat"]]},
        "TPE ": vlib["banking"],
        "status": vlib["status"] == "OPEN",
        "last update": vlib["last_update"] })
    
client = MongoClient("mongodb+srv://moussa:M0USS4@cluster0.bsttv.mongodb.net/test")
db = client.test

for vLyon in vLyon_to_insert:
    db.vLyon.insert_one(vLyon)
for vlille in vlilles_to_insert:
    db.vLille.insert_one(vlille)
for vParis in vParis_to_insert:
    db.vParis.insert_one(vParis)
for vRennes in vRennes_to_insert:
    db.vRennes.insert_one(vRennes)

db.test.vLyon.insert_many(vLyon_format).inserted_ids   
