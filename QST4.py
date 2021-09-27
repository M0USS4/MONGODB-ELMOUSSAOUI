import requests
import json
from pprint import pprint
from pymongo import MongoClient
import time
import dateutil.parser
from bson.objectid import ObjectId

client = MongoClient("mongodb+srv://moussa:M0USS4@cluster0.bsttv.mongodb.net/test")
db=client.test
bdd=db.records

def getByname(nom):
    db.vlille.create_index([("name", "text")])
    stations=db.vlille.find(
        {
            "$text":{
                "$search": nom
                }
            }
        )  
        
    nearstation = []
    for station in stations:
        datas = [
                    {
                    "name":station.get('name'),
                    "coordinates":station.get('geometry',{}).get('coordinates'),
                    "bike":get_bikebyid(station.get('_id')),
                    "stand":get_standbyid(station.get('_id')),
                    "_id":station.get('_id')
                    }
                ]
        nearstation.append(datas)
    return nearstation

def get_bikebyid(id):
    try:
        tps = db.vlille.find_one({ "station_id": id }, { 'bike_availbale': 1 })
        return tps['bike_availbale']
    except :
        return -1


def get_standbyid(id):
    try:
        tps = db.lille.find_one({ "station_id": id }, { 'stand_availbale': 1 })
        return tps['stand_availbale']
    except :
        return -1


def update_Stations_Name(id,newName):
    try:
        db.vlille.update(
        {"_id":id},
        {"$set": {'name':newName}})
    except :
        pprint("error update")
        pass


def delete_station_Data(id):
    db.vlille.delete_one(
        {"_id":id}
    )
    db.test.delete_one(
        {"station_id":id}
    )

def update_boolean_activate_station():
    {
        db.vlille.update_many(
            {},
            {"$set": {"activate":True}},upsert=False, array_filters=None)
    }

    
def query_polygone(x0,x1,x2,x3,state):
    db.vlille.update_many(
        {"geometry": {
            "$geoWithin": 
                { 
                    "$polygon": [ [ x0[0] , x0[1] ], [ x1[0] , x1[1]], [ x2[0] , x2[1] ],[x3[0] , x3[1]] ] 
                }
            }
        },
        {"$set": {"activate":state}})

def give_all_stations_with_etc():
    printall=db.vlille.find()
    for elem in printall:
        print(elem)
    test=db.vlille.aggregate([
        {
            "$project":
            {
                "name":1,
                "isLower":{"$lt": ["$ratio",20 ]},
                }
            }
        ])
    stations=[]
    for elem in test:
        if(elem['isLower']==True):
            name=elem["name"]
            stations.append(name)
    print(stations)

def ratio():
    stations_list = db.records.aggregate([
        {
            '$lookup': {
                'from': 'vlille',
                'localField': "_id",
                'foreignField': "station_id",
                'as': "station"
                }, 
            'pipeline': [
                {
                    '$match': {  
                        '$expr': {'$eq': ['$_id', '$station_id']}  
                    }
                },
                {
                "$project":
                {
                    "name":'$name',
                    "size": [{'$sum':['$bike_availbale','$stand_availbale']}],
                    "ratio":{
                        '$cond':{
                            'if': {'$sum': ['$nbvelosdispo', '$nbplacesdispo']} == 0},  
                            'then': 0.0,
                            'else': {'$divide': ['$bike_availbale', {'$sum': ['$bike_availbale', '$stand_availbale']}]}
                        }
                    }     
                },
                {  
                        '$match': {
                            'ratio': {'$lte': '$ratio'}
                        }
                }
            ]
         }
    ])

x0=[3.048082,50.650289]
x1=[3.024833,50.621240]
x2=[3.086996,50.617326]
x3=[3.070742,50.651181]
#query_polygone(x0,x1,x2,x3,True)


ratio()
##update_boolean_activate_station()

##delete_station_Data(toUpdate[0][0].get('_id'))
##update_Stations(toUpdate[0][0].get('_id'),"Seine - Flandre")



