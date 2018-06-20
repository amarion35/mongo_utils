from pymongo import MongoClient
from pandas import DataFrame
import json
import pandas as pd
import numpy as np
np.set_printoptions(threshold=5)

HOST = '192.168.2.23'

def add_subj_scores_to_metrics():
    client = MongoClient(HOST)
    db = client.ai_vqa

    subj_metrics = ['MOS', 'MOS_STD', 'DMOS', 'DMOS_STD']

    meta = db.metadatas.find()
    for doc in meta:
        plouf = {s: doc[s] for s in subj_metrics}
        db.metrics.update_one({'FILE_NAME': doc['FILE_NAME']},{'$set':plouf},upsert=False)
    

    client.close()

def set_datasets_to_metadatas():
    client = MongoClient(HOST)
    db = client.ai_vqa

    metadatas = db.metadatas.find({}, {'FILE_NAME':1})
    konvid = []
    live = []
    csiq = []
    irccyn = []
    for doc in metadatas:
        if doc['FILE_NAME'].endswith('.mp4'):
            konvid.append(doc)
        elif doc['FILE_NAME'].endswith('fps.yuv'):
            live.append(doc)
        elif doc['FILE_NAME'].endswith('0.yuv'):
            csiq.append(doc)
        elif doc['FILE_NAME'].endswith('1.yuv'):
            csiq.append(doc)
        elif doc['FILE_NAME'].endswith('2.yuv'):
            csiq.append(doc)
        elif doc['FILE_NAME'].endswith('3.yuv'):
            csiq.append(doc)
        elif doc['FILE_NAME'].endswith('4.yuv'):
            csiq.append(doc)
        elif doc['FILE_NAME'].endswith('5.yuv'):
            csiq.append(doc)
        elif doc['FILE_NAME'].endswith('6.yuv'):
            csiq.append(doc)
        elif doc['FILE_NAME'].endswith('7.yuv'):
            csiq.append(doc)
        elif doc['FILE_NAME'].endswith('8.yuv'):
            csiq.append(doc)
        elif doc['FILE_NAME'].endswith('9.yuv'):
            csiq.append(doc)
        elif doc['FILE_NAME'].endswith('ref.yuv'):
            csiq.append(doc)
        else:
            irccyn.append(doc)

    print('konvid: ' + str(len(konvid)))
    print('live: ' + str(len(live)))
    print('csiq: ' + str(len(csiq)))
    print('irccyn: ' + str(len(irccyn)))

    for doc in konvid:
        db.metadatas.update_one({'FILE_NAME': doc['FILE_NAME']},{'$set':{'DATASET':'konvid'}},upsert=False)  
    for doc in live:
        db.metadatas.update_one({'FILE_NAME': doc['FILE_NAME']},{'$set':{'DATASET':'live'}},upsert=False)  
    for doc in csiq:
        db.metadatas.update_one({'FILE_NAME': doc['FILE_NAME']},{'$set':{'DATASET':'csiq'}},upsert=False)  
    for doc in irccyn:
        db.metadatas.update_one({'FILE_NAME': doc['FILE_NAME']},{'$set':{'DATASET':'irccyn'}},upsert=False)   

    client.close()

def remove_field_from_collection(field, collection):
    client = MongoClient(HOST)
    db = client.ai_vqa

    db[collection].update_many({}, {'$unset': {field:1}}, upsert=False)

    client.close()

def rename_field(collection, field, name):
    client = MongoClient(HOST)
    db = client.ai_vqa
    
    db[collection].update_many({}, {'$rename':{field:name}}, upsert=False)

    client.close()

def map_field(collection, field, function):
    client = MongoClient(HOST)
    db = client.ai_vqa

    field_values = db[collection].find({field:{'$exists':True}}, {'_id':1, field:1})
    field_values = np.array(list(field_values))
    new_values = list(map(function, field_values))
    for doc in new_values:
        db[collection].update_many({'_id': doc['_id']}, {'$set':{field: doc[field]}}, upsert=False)
    
    client.close()


def main():
    rename_field('metrics', 'TEST-SCORE', 'TEST1-SCORE')


if __name__ == '__main__':
    main()