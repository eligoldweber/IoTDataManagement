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
SAMPLE_PASS = False
CLEAN_PASS = False
COMPRESS_PASS = False
ZLIB_COMPRESS = True
GRAPH = False
THRESHOLD = 50 # %difference in the length
LIMIT = 30 # Upper bound for B+D

#Analysis
totalBytes = 0
entriesInDB = 0
nocomprdata = 0
comprdata = 0
comprdatanozlib = 0

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
				data[attr] = str(convertDate(getattr(self,attr)) - int(other[attr]))
			elif isinstance(value, float):
				data[attr] = round( getattr(self,attr) - other[attr] , 2)
			else:
				data[attr] = getattr(self,attr) - other[attr]
		return data

app = faust.App(
    'node-data',
    broker='kafka://localhost:9092',
    key_serializer='json',
    store='rocksdb://',
)

rawDataTopic = app.topic('nodeInput',value_type=Point,value_serializer='json')
CleanDataTopic = app.topic('clean-data',value_type=Point,value_serializer='json')
CompressDataTopic = app.topic('compress-data',value_type=Point,value_serializer='json')
NoCompressDataTopic = app.topic('non-compress-data',value_type=Point,value_serializer='json')
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
			if(COMPRESS_PASS):
				await NoCompressDataTopic.send(value=data)
			else:
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
					if(COMPRESS_PASS):
						await NoCompressDataTopic.send(value=data)
					else:
						await CompressDataTopic.send(value=data)
			#with MAD detection
			# mad.add_number(data.temp)
# 			if not mad.outlier(data.temp):
# 				await CompressDataTopic.send(value=data)


@app.agent(CompressDataTopic)
async def processCompressDataNew(cleanData):
	global totalBytes
	global nocomprdata
	currentBase = data = CompressedData = {}
	current = id = 1
	delta = {}

	async for data in cleanData:
		nocomprdata = nocomprdata + sys.getsizeof(data)
		if id == 1:
			CompressedData = currentBase = copydata(data)				
			CompressedData['id'] = id
			id = id + 1
			delta = {}
			for attr, value in data.__dict__.items():
				if attr != 'uid' and attr != 'rawId':
					delta[attr] = []
			CompressedData['D'] = delta
		elif checkThreshold(currentBase,data) or current == LIMIT or sizecheck(CompressedData):
			print("clean data " + str(nocomprdata))
			print('number of entries' + str(current))
			putInDB(CompressedData)
			CompressedData = currentBase = copydata(data)				
			CompressedData['id'] = id
			id = id + 1
			delta = {}
			for attr, value in data.__dict__.items():
				if attr != 'uid' and attr != 'rawId':
					delta[attr] = []
			CompressedData['D'] = delta
			current = 1
		else:
			tmp = data-currentBase
			for attr, value in data.__dict__.items():
				if attr != 'uid' and attr != 'rawId':
					delta[attr].append(tmp[attr])
			CompressedData['D'] = {}
			CompressedData['D'] = delta
			current = current + 1

	putInDB(CompressedData)
		
@app.agent(NoCompressDataTopic)
async def processNoCompressDataNew(cleanData):
	global totalBytes
	global entriesInDB
	async for data in cleanData:
		db.put(bytes(str(data.uid), encoding= 'utf-8'), bytes(data.dumps()))
		print(sys.getsizeof(bytes(data.dumps())))
		totalBytes = totalBytes + sys.getsizeof(bytes(data.dumps()))
		entriesInDB = entriesInDB + 1
		print(db.get(bytes(str(data.uid), encoding= 'utf-8')))
		stats = "[MONITOR] average runtime events: "+ str(app.monitor.events_runtime_avg)
		
@app.task()
async def produce():
	chunksize = 1
	i = 0
	for chunk in pd.read_csv('./dataSets/BeachMulti1000_3Streams.csv', chunksize=chunksize):
		d = Point("",0,0,0,0)
		for index, row in chunk.head().iterrows():
			d = Point(ts=row['Measurement Timestamp'],temp=row['Air Temperature'],wind=row['Wind Speed'],rawId=i,uid=i)
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
		for k in list(it):
			db.delete(k)
		# prints [b'key1', b'key2', b'key3']
		print (list(it))
		
	async def on_stop(self):
		global totalBytes
		global entriesInDB
		if GRAPH:
			graphDataFromDB()
		print('BACKGROUNDSERVICE IS STOPPING')
		cnt = 0
		it = db.iterkeys()
		it.seek_to_first()
		for k in list(it):
			print("Deleteing: {0}".format(k))
			db.delete(k)
			cnt = cnt +1
		print("Total entries in DB :: " + str(entriesInDB))
		print("Total Bytes stored = " + str(totalBytes))


'''06/22/2015 07:00:00 PM'''
def convertDate (ts):
	dt = datetime.strptime(ts,'%m/%d/%Y %I:%M:%S %p')
	""" Return time in minutes"""
	return int(dt.timestamp()/60)
	
def putInDB (CompressedData):
	if(not COMPRESS_PASS):
		global totalBytes
		global entriesInDB
		global dbentry
		global comprdata
		global comprdatanozlib
		#CompressedData['Delta'] = delta
		if ZLIB_COMPRESS:
			print(CompressedData)
			print('currernt entry before zlib ' + str(sys.getsizeof(CompressedData)))
			data = zlib.compress(str(CompressedData).encode('utf-8'), 2)
			db.put(bytes(str(CompressedData['id']), encoding= 'utf-8'), bytes(data))
			totalBytes = totalBytes + sys.getsizeof(bytes(data))
			comprdata = comprdata + sys.getsizeof(data)
			comprdatanozlib = comprdatanozlib + (sys.getsizeof(str(CompressedData)))
			print("comprdata " + str(comprdata))
			print("totalBytes " + str(totalBytes))
			print('currernt entry with zlib ' + str(sys.getsizeof(data)))
			print('compressed data with zlib ' + str(comprdata))
			print('compressed data without zlib ' + str(comprdatanozlib))
		else:
			db.put(bytes(str(CompressedData['id']), encoding= 'utf-8'), bytes(str(CompressedData),encoding= 'utf-8'))
			totalBytes = totalBytes + sys.getsizeof(bytes(str(CompressedData),encoding= 'utf-8'))
			comprdata = comprdata + sys.getsizeof(CompressedData)
			print("Compressed Data " + str(comprdata))
		entriesInDB = entriesInDB + 1
		print(entriesInDB)
		#print(db.get(bytes(str(CompressedData['id']), encoding= 'utf-8')))
		stats = "[MONITOR] average runtime events: "+ str(app.monitor.events_runtime_avg)
		#print(stats)

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
        
def sizecheck(CompressedData):
	if ZLIB_COMPRESS:
		tmp = zlib.compress(str(CompressedData).encode('utf-8'), 2)
		if sys.getsizeof(tmp) > 255: 
			return True
		else:
			return False
	else:
		if sys.getsizeof(str(CompressedData)) > 255: 
			return True
		else:
			return False

def graphDataFromDB():
	global entriesInDB
	print("GRAPHING " + str(entriesInDB) + " entries:")
	dfGraph = pd.DataFrame(columns=['TimeSeries','Air_Temperature','Wind_Speed'])
	it = db.iterkeys()
	it.seek_to_first()
	i = 0
	for k in list(it):
		temp = json.loads(db.get(k))
		dfGraph.loc[i] = [temp['rawId'],temp['temp'],temp['wind']]
		i = i + 1
		
	dfGraph = dfGraph.sort_values(by=['TimeSeries'])
	ax = plt.gca()
	dfGraph.plot(color='blue',x='TimeSeries', y='Air_Temperature',ylim=(0,35),figsize=(25,10))
	plt.savefig('currentData_Air.png')
	dfGraph.plot(color='black',x='TimeSeries', y='Wind_Speed',ylim=(-1,11),figsize=(25,10))
	plt.savefig('currentData_Wind.png')
	
 	
	plt.savefig('currentData.png')

if __name__ == '__main__':
    app.main()
