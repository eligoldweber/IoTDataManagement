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
import zlib
from MAD import MAD
from KNN import KNN
from DSample import DSample 

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
	wind: float
	rawId: int
	uid: int
	
class CompressedPoint(faust.Record, serializer='json'):
	ts: int
	temp: int
	id: int
	delta = []
	
	def __init__(self):
		pass

class Delta (faust.Record, serializer='json'):
	def __init__(self, ts, temp):  
		self.ts = ts  
		self.temp = temp
		
		
app = faust.App(
    'node-data',
    broker='kafka://localhost:9092',
    key_serializer='json',
    store='rocksdb://',
)
SAMPLE_PASS = False
CLEAN_PASS = False
COMPRESS_PASS = True
THRESHOLD = 5 #length difference in B+D to start new B
LIMIT = 20 # Upper bound for B+D
currentBase = CompressedPoint() 
rawDataTopic = app.topic('nodeInput',value_type=Point,value_serializer='json')
CleanDataTopic = app.topic('clean-data',value_type=Point,value_serializer='json')
CompressDataTopic = app.topic('compress-data',value_type=Point,value_serializer='json')
db = rocksdb.DB("test.db", rocksdb.Options(create_if_missing=True,num_levels=1,target_file_size_base=2048))
knn = KNN()
mad = MAD()
tempSampler = DSample(sampleRate,k,tMax,windowSize,phi)
windSampler = DSample(sampleRate,k,tMax,windowSize,phi)


@app.agent(rawDataTopic)
async def processData(rawData):
	i = 0
	global tempSample
	global windSample
	async for data in rawData:
		if(SAMPLE_PASS):
			await CleanDataTopic.send(value=data)
		else:
			sampledTemp = tempSampler.add_val(data.uid,data.temp)
			sampledWind = windSampler.add_val(data.uid,data.wind)
			if(sampledWind[0] or sampledTemp[0]):
				data.uid = i
				data.temp = sampledTemp[2]
				data.wind = sampledWind[2]
				await CleanDataTopic.send(value=data)
				i = i + 1
				time.sleep(.005)
			

@app.agent(CleanDataTopic)
async def processCleanData(rawData):
	global knn
	global mad
	async for data in rawData:
		if(CLEAN_PASS):
			await CompressDataTopic.send(value=data)
		else:
		    # print("Send to Compress")
		    # data is a point
    
		    # # with KNN
			val = [data.temp,data.wind]
			knn.add_number(val)
			if len(knn.data) < knn.k:
				await CompressDataTopic.send(value=data)
			else:
				if(not knn.outlier(val)):
				   await CompressDataTopic.send(value=data)
			#with MAD detection
			# mad.add_number(data.temp)
# 			if not mad.outlier(data.temp):
# 				await CompressDataTopic.send(value=data)


@app.agent(CompressDataTopic)
async def processCompressDataNew(cleanData):
	CompressedData = CompressedPoint()
	current = 1
	id = 1
	delta = []

	async for data in cleanData:
		if(COMPRESS_PASS):
			db.put(bytes(str(data.uid), encoding= 'utf-8'), bytes(str(data.dumps()), encoding= 'utf-8'))
			print(db.get(bytes(str(data.uid), encoding= 'utf-8')))
			stats = "[MONITOR] average runtime events: "+ str(app.monitor.events_runtime_avg)
			# print(stats)
		else:
			if id == 1:
				CompressedData.ts = currentBase.ts = convertDate(data.ts)
				CompressedData.temp = currentBase.temp = data.temp
				CompressedData.id = id
				id = id + 1
				delta = []
				print(len(str(convertDate(data.ts) - currentBase.ts)))
			elif len(str(convertDate(data.ts) - currentBase.ts)) > THRESHOLD or current == LIMIT:
				putInDB(CompressedData,delta)
				CompressedData.ts = currentBase.ts = convertDate(data.ts)
				CompressedData.temp = currentBase.temp = data.temp
				CompressedData.id = id
				id = id + 1
				delta = []
				current = 1
			else:
				tmpts = convertDate(data.ts) - currentBase.ts
				delta.append(Delta(tmpts,round(data.temp - currentBase.temp, 3)))
				current = current + 1

			putInDB(CompressedData,delta)
			
@app.task()
async def produce():
	chunksize = 1
	i = 0
	for chunk in pd.read_csv('beachSampleDataMulti.csv', chunksize=chunksize):
		d = Point("",0,0,0,0)
		for index, row in chunk.head().iterrows():
			d = Point(ts=row['Measurement Timestamp'],temp=row['Air Temperature'],wind=row['wind'],rawId=i,uid=i)
			i = i + 1
			await rawDataTopic.send(value=d)
			time.sleep(.005)
            

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
	""" Return time in minutes"""
	return dt.timestamp()/60
	
def putInDB (CompressedData, delta):
	CompressedData.delta = delta
	data = zlib.compress(str(CompressedData).encode('utf-8'), 2)
	db.put(bytes(str(CompressedData.id), encoding= 'utf-8'), bytes(str(CompressedData), encoding= 'utf-8'))
	#db.put(bytes(str(CompressedData.id), encoding= 'utf-8'), bytes(str(data), encoding= 'utf-8'))
	print(db.get(bytes(str(CompressedData.id), encoding= 'utf-8')))
	stats = "[MONITOR] average runtime events: "+ str(app.monitor.events_runtime_avg)
	print(stats)
	
            
if __name__ == '__main__':
    app.main()



