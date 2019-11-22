#!/usr/bin/env python
import faust
from mode import Service
import pandas as pd
import time
import json
import Record
import rocksdb


class Point(faust.Record, serializer='json'):
    ts: str
    temp: float
    id: int
    
app = faust.App(
    'node-data',
    broker='kafka://localhost:9092',
    key_serializer='json',
    store='rocksdb://',
)

rawDataTopic = app.topic('nodeInput',value_type=Point,value_serializer='json')
CleanDataTopic = app.topic('clean-data',value_type=Point,value_serializer='json')
CompressDataTopic = app.topic('compress-data',value_type=Point,value_serializer='json')
db = rocksdb.DB("test.db", rocksdb.Options(create_if_missing=True))

@app.agent(rawDataTopic)
async def processData(rawData):
    async for data in rawData:
        print("Send to Clean")
        await CleanDataTopic.send(value=data)


@app.agent(CleanDataTopic)
async def processCleanData(rawData):
    async for data in rawData:
        print("Send to Compress")
        await CompressDataTopic.send(value=data)
        
        
@app.agent(CompressDataTopic)
async def processCompressData(cleanData):
    async for data in cleanData:
        db.put(bytes(str(data.id), encoding= 'utf-8'), bytes(str(data.dumps()), encoding= 'utf-8'))
        print(db.get(bytes(str(data.id), encoding= 'utf-8')))
        stats = "[MONITOR] average runtime events: "+ str(app.monitor.events_runtime_avg)
        print(stats)

@app.task()
async def produce():
    i = 0
    chunksize = 1
    for chunk in pd.read_csv('beachsampleRateData_Smaller.csv', chunksize=chunksize):
        d = Point("",0,0)
        for index, row in chunk.head().iterrows():
             d = Point(ts=row['Measurement Timestamp'],temp=row['Air Temperature'],id=i)
             i = i + 1
             

        await rawDataTopic.send(value=d)
        time.sleep(.0500)
            

@app.service
class BackgroundService(Service):

    async def on_start(self):
        print('BACKGROUND SERVICE IS STARTING')
        print('CURRENT Keys in DB:')
        it = db.iterkeys()
        it.seek_to_first()
        # prints [b'key1', b'key2', b'key3']
        print (list(it))

    async def on_stop(self):
        print('BACKGROUNDSERVICE IS STOPPING')
        it = db.iterkeys()
        it.seek_to_first()
        for k in list(it):
            print("Deleteing: {0}".format(k))
            db.delete(k)


            
            
if __name__ == '__main__':
    app.main()




