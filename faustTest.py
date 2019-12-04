#!/usr/bin/env python
import faust
from mode import Service
import pandas as pd
import time
import json
import Record
import rocksdb
from datetime import datetime

class Point(faust.Record, serializer='json'):
    ts: str
    temp: float
    id: int

class CompressedPoint(faust.Record, serializer='json'):
    ts: int
    temp: float
    flag: str
    id: int

    def __init__(self):
        pass
    
app = faust.App(
    'node-data',
    broker='kafka://localhost:9092',
    key_serializer='json',
    store='rocksdb://',
)
LIMIT = 20
currentBase = CompressedPoint() 
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
        
'''     
@app.agent(CompressDataTopic)
async def processCompressDataOld(cleanData):
    async for data in cleanData:
        db.put(bytes(str(data.id), encoding= 'utf-8'), bytes(str(data.dumps()), encoding= 'utf-8'))
        print(db.get(bytes(str(data.id), encoding= 'utf-8')))
        stats = "[MONITOR] average runtime events: "+ str(app.monitor.events_runtime_avg)
        print(stats)
'''

@app.agent(CompressDataTopic)
async def processCompressDataNew(cleanData):
    CompressedData = CompressedPoint()
    current = LIMIT
    async for data in cleanData:
        if current == LIMIT:
            CompressedData.flag = 'B'
            CompressedData.ts = currentBase.ts = convertDate(data.ts)
            CompressedData.temp = currentBase.temp = data.temp
        else:
            CompressedData.flag = 'D'
            CompressedData.ts = convertDate(data.ts) - currentBase.ts 
            CompressedData.temp = data.temp - currentBase.temp

        CompressedData.id = data.id

        db.put(bytes(str(CompressedData.id), encoding= 'utf-8'), bytes(str(CompressedData), encoding= 'utf-8'))
        print(db.get(bytes(str(CompressedData.id), encoding= 'utf-8')))
        stats = "[MONITOR] average runtime events: "+ str(app.monitor.events_runtime_avg)
        print(stats)

        if (current - 1) == 0:
            current = LIMIT
        else:
            current = current - 1

@app.task()
async def produce():
    i = 0
    chunksize = 1
    for chunk in pd.read_csv('beachSampleData_Smaller.csv', chunksize=chunksize):
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

'''06/22/2015 07:00:00 PM'''
def convertDate (ts):
    dt = datetime.strptime(ts,'%m/%d/%Y %I:%M:%S %p')
    """ Return time in seconds"""
    return dt.timestamp()

	
            
            
if __name__ == '__main__':
    app.main()



