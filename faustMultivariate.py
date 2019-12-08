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

# Dynamic Sample Constants -- try changing them to see differences 
windowSize = 35
k = .9
tMax = 20
phi = 2
sampleRate = 1
SAMPLE_PASS = True
CLEAN_PASS = True
COMPRESS_PASS = True
ZLIB_COMPRESS = False
THRESHOLD = 50 # %difference in the length
LIMIT = 20 # Upper bound for B+D

#Analysis
totalBytes = 0;

class Point(faust.Record, serializer='json'):
	ts: str
	temp: float
	wind: float
	rawId: int
	uid: int
	
	def __sub__(self, other):
		data = {}
		for attr, value in self.__dict__.items():
			if attr == 'uid' or attr == 'rawId':
				pass
			elif attr == 'ts':
				data[attr] = str(convertDate(getattr(self,attr)) - float(other[attr]))
			elif isinstance(value, float):
				data[attr] = round( getattr(self,attr) - other[attr] , 2)
			else:
				data[attr] = getattr(self,attr) - other[attr]
		return data

class CompressedPoint(faust.Record, serializer='json'):
	ts: str
	temp: int
	wind: float
	id: int
	delta = []
	
	def __init__(self):
		pass
		
app = faust.App(
    'node-data',
    broker='kafka://localhost:9092',
    key_serializer='json',
    store='rocksdb://',
)

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
	global totalBytes
	currentBase = data = CompressedData = Point("",0,0,0,0)
	current = id = 1
	delta = []

	async for data in cleanData:
		if(COMPRESS_PASS):
			db.put(bytes(str(data.uid), encoding= 'utf-8'), bytes(str(data.dumps()), encoding= 'utf-8'))
			totalBytes = totalBytes + sys.getsizeof(bytes(str(data.dumps()), encoding= 'utf-8'))
			print(db.get(bytes(str(data.uid), encoding= 'utf-8')))
			stats = "[MONITOR] average runtime events: "+ str(app.monitor.events_runtime_avg)
		else:
			if id == 1:
				CompressedData = currentBase = copydata(data)				
				CompressedData['id'] = id
				id = id + 1
				delta = []
			elif ((checkThreshold(currentBase,data) and current > LIMIT/2) or current == LIMIT):
				putInDB(CompressedData,delta)
				CompressedData = currentBase = copydata(data)				
				CompressedData['id'] = id
				id = id + 1
				delta = []
				current = 1
			else:
				delta.append(data-currentBase)
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
		global totalBytes
		print('BACKGROUNDSERVICE IS STOPPING')
		cnt = 0
		it = db.iterkeys()
		it.seek_to_first()
		for k in list(it):
			print("Deleteing: {0}".format(k))
			db.delete(k)
			cnt = cnt +1
		print("Total entries in DB :: " + str(cnt))
		print("Total Bytes stored = " + str(totalBytes))


'''06/22/2015 07:00:00 PM'''
def convertDate (ts):
	dt = datetime.strptime(ts,'%m/%d/%Y %I:%M:%S %p')
	""" Return time in minutes"""
	return dt.timestamp()/60
	
def putInDB (CompressedData, delta):
	global totalBytes
	CompressedData['Delta'] = delta
	if ZLIB_COMPRESS:
		data = zlib.compress(str(CompressedData).encode('utf-8'), 2)
		db.put(bytes(str(CompressedData['id']), encoding= 'utf-8'), bytes(str(data), encoding= 'utf-8'))
		totalBytes = totalBytes + sys.getsizeof(bytes(str(data), encoding= 'utf-8'))
	else:
		db.put(bytes(str(CompressedData['id']), encoding= 'utf-8'), bytes(str(CompressedData), encoding= 'utf-8'))
		totalBytes = totalBytes + sys.getsizeof(bytes(str(CompressedData), encoding= 'utf-8'))
	print(db.get(bytes(str(CompressedData['id']), encoding= 'utf-8')))
	stats = "[MONITOR] average runtime events: "+ str(app.monitor.events_runtime_avg)
	print(stats)

def checkThreshold(currentBase,data):
	for attr, value in data.__dict__.items():
		delta = data - currentBase
		#if attr != 'uid' and attr != 'rawId':
		if attr == 'ts':
			if len(str(delta[attr])) > (THRESHOLD/100) * len(str(currentBase[attr])):
				return True
	return False
   
def copydata(data):
	compressed = {}
	for attr, value in data.__dict__.items():
		if attr != 'uid' and attr != 'rawId':
			if attr == 'ts':
				compressed[attr] = str(convertDate(getattr(data,attr)))
			else:	
				compressed[attr] = getattr(data,attr)			
	return compressed
         
if __name__ == '__main__':
    app.main()


