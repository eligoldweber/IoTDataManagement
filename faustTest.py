#!/usr/bin/env python
import faust
from mode import Service
import pandas as pd
import time
import json
import Record

class Point(faust.Record, serializer='json'):
    ts: str
    v: float
    

app = faust.App(
    'node-data',
    broker='kafka://localhost:9092',
    key_serializer='json',
)

rawDataTopic = app.topic('nodeInput',value_type=Point,value_serializer='json')


@app.agent(rawDataTopic)
async def processData(rawData):
    async for data in rawData:
        print(data.dumps())



@app.timer(interval=1.0)
async def produce():
    chunksize = 1
    for chunk in pd.read_csv('beachSampleData_Smaller.csv', chunksize=chunksize):
        d = Point("",0)
        for index, row in chunk.head().iterrows():
             d = Point(row['Measurement Timestamp'],row['Air Temperature'])

        await rawDataTopic.send(value=d)
        time.sleep(.0500)
            

if __name__ == '__main__':
    app.main()




