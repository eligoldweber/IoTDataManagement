#!/usr/bin/env python
import faust
import pandas as pd
import time
import json

app = faust.App(
    'node-data',
    broker='kafka://localhost:9092',
)

rawDataTopic = app.topic('nodeInput', value_type=str)


@app.agent(rawDataTopic)
async def processData(rawData):
    async for data in rawData:
        print(data)


@app.timer(5)
async def produce():
    chunksize = 1
    for chunk in pd.read_csv('beachSampleData.csv', chunksize=chunksize):
        for c in chunk["Measurement Timestamp"]:
            await processData.send(value=f'{c}')
            time.sleep(.0500)
            

if __name__ == '__main__':
    app.main()
