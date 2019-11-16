#!/usr/bin/env python
import faust
from mode import Service
import pandas as pd
import time
import json
import Record

class Point(faust.Record, serializer='json'):
    ts: str
    temp: float
    id: int
    

app = faust.App(
    'node-data',
    broker='kafka://localhost:9092',
    key_serializer='json',
)

rawDataTopic = app.topic('nodeInput',value_type=Point,value_serializer='json')
CleanDataTopic = app.topic('clean-data',value_type=Point,value_serializer='json')
CompressDataTopic = app.topic('compress-data',value_type=Point,value_serializer='json')


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
        print(data.dumps())

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
            

            
if __name__ == '__main__':
    app.main()




