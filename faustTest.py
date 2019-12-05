#!/usr/bin/env python
import faust
from mode import Service
import pandas as pd
import time
import json
import Record
import rocksdb
import math
import sys
from datetime import datetime
import matplotlib.pyplot as plt
from MAD import MAD
from KNN import KNN


#Constants -- try changing them to see differences 
windowSize = 35
k = .9
tMax = 20
phi = 2
sampleRate = 1


dfDynamicSample = pd.DataFrame(columns=['id','tempVal','Rolling_Average','Rolling_STD','bb_UP','bb_DOWN','sampleRate'])


class Point(faust.Record, serializer='json'):
	ts: str
	temp: float
	rawId: int
	uid: int
	
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
db = rocksdb.DB("test.db", rocksdb.Options(create_if_missing=True,num_levels=1,target_file_size_base=2048))
knn = KNN()
mad = MAD()

@app.agent(rawDataTopic)
async def processData(rawData):
    global sampleRate
    global dfDynamicSample
    dfRaw = pd.DataFrame(columns=['id','tempVal'])
    i = 0
    uidCnt = 0
    chunksize = 1
    async for data in rawData:
		# Only 'save' data based on sampleRate (default is 1 ie every value is saved)
        # print("-----> " + str(sampleRate) + " :: processing " + str(data.uid))
        if(i % sampleRate == 0):
			# Currently only saving an 'id' and 1 value 'Air Temp' (Needs to be adjusted for bigger examples)
            dfRaw.loc[i] = [i,data.temp]
            if(len(dfRaw.index) >= windowSize):
                # More robust df that calcuates meta data about rolling frame
                dfStats = dfRaw.copy()
                dfStats['Rolling_Average'] = dfStats.iloc[:,1].rolling(window=windowSize).mean()
                dfStats['Rolling_STD'] = dfStats.iloc[:,1].rolling(window=windowSize).std()
                dfStats['bb_UP'] = dfStats.iloc[:,2] + (k*dfStats.iloc[:,3])
                dfStats['bb_DOWN'] = dfStats.iloc[:,2] - (k*dfStats.iloc[:,3])
                #Calc distance between upper BB and lower BB to use in sampling rate
                std = float(dfStats.tail(1)['Rolling_STD'])
                up = float(dfStats.tail(1)['bb_UP'])
                down = float(dfStats.tail(1)['bb_DOWN'])
                dyn = round(2*k*std,4)
                assert dyn == round(abs(up - down),4)
                rawsampleRate = (tMax)/(1+pow(dyn,phi))
                # Cant sample less than every data point, so rouind up
                if(rawsampleRate < 1):
                    sampleRate = math.ceil(rawsampleRate)
                else:
                    sampleRate = round(rawsampleRate)
                    
                dfStats['sampleRate'] = sampleRate
                # Add new row to DF to keep track of total data (Used for diagram -- not needed in real life)
                dfDynamicSample =dfDynamicSample.append(dfStats.tail(1))
                dfRaw = dfRaw.drop(dfRaw.index[0])
                data.uid = uidCnt
                uidCnt = uidCnt +1 
                await CleanDataTopic.send(value=data)
                time.sleep(.050)
            else:
                data.uid = uidCnt
                uidCnt = uidCnt +1
                await CleanDataTopic.send(value=data)
                time.sleep(.050)
        i = i + 1
    # axDynamic = plt.gca()
#     dfDynamicSample.plot(kind='line',x='id',y='tempVal',color='green',ax=axDynamic,figsize=(25,10))
#     dfDynamicSample.plot(kind='line',x='id',y='Rolling_Average',color='black',ax=axDynamic,figsize=(25,10))
#     dfDynamicSample.plot(kind='line',x='id',y='bb_UP',color='blue',ax=axDynamic)
#     dfDynamicSample.plot(kind='line',x='id',y='bb_DOWN',color='red',ax=axDynamic)
#     dfDynamicSample.plot(kind='line',x='id',y='sampleRate',color='orange',ax=axDynamic,alpha=0.5)
#
#     plt.savefig('isItWorking.png')


@app.agent(CleanDataTopic)
async def processCleanData(rawData):
	global knn
	global mad
	async for data in rawData:
        # print("Send to Compress")
        # data is a point
        
        # # with KNN
 #        val = [data.temp]
 #        knn.add_number(val)
 #        if len(knn.data) < knn.k:
 #            await CompressDataTopic.send(value=data)
 #        else:
 #            if(not knn.outlier(val)):
 #                await CompressDataTopic.send(value=data)
		#with MAD detection
		mad.add_number(data.temp)
		if not mad.outlier(data.temp):
			await CompressDataTopic.send(value=data)


        
# @app.agent(CompressDataTopic)
# async def processCompressData(cleanData):
# 	async for data in cleanData:
# 		db.put(bytes(str(data.uid), encoding= 'utf-8'), bytes(str(data.dumps()), encoding= 'utf-8'))
# 		print(db.get(bytes(str(data.uid), encoding= 'utf-8')))
# 		stats = "[MONITOR] average runtime events: "+ str(app.monitor.events_runtime_avg)
# 		print(stats)


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

		CompressedData.id = data.uid
		db.put(bytes(str(CompressedData.id), encoding= 'utf-8'), bytes(str(CompressedData), encoding= 'utf-8'))
		print(db.get(bytes(str(CompressedData.id), encoding= 'utf-8')))
		stats = "[MONITOR] average runtime events: "+ str(app.monitor.events_runtime_avg)
        # print(stats)

		if (current - 1) == 0:
			current = LIMIT
		else:
			current = current - 1
			
@app.task()
async def produce():
    global sampleRate
    global dfDynamicSample
    dfRaw = pd.DataFrame(columns=['id','tempVal'])
    i = 0
    uidCnt = 0
    chunksize = 1
    for chunk in pd.read_csv('beachSampleData.csv', chunksize=chunksize):
        d = Point("",0,0,0)
        for index, row in chunk.head().iterrows():
            d = Point(ts=row['Measurement Timestamp'],temp=row['Air Temperature'],rawId=i,uid=i)
            i = i + 1
            await rawDataTopic.send(value=d)
            time.sleep(.050)
            

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
		cnt = 0
		it = db.iterkeys()
		it.seek_to_first()
		for k in list(it):
			print("Deleteing: {0}".format(k))
			db.delete(k)
			cnt = cnt +1
		print("Total count :: " + str(cnt))


'''06/22/2015 07:00:00 PM'''
def convertDate (ts):
	dt = datetime.strptime(ts,'%m/%d/%Y %I:%M:%S %p')
	""" Return time in seconds"""
	return dt.timestamp()
	
            
if __name__ == '__main__':
    app.main()



