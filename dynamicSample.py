#!/usr/bin/env python
import pandas as pd
import matplotlib.pyplot as plt
import math

#Constants -- try changing them to see differences 
windowSize = 20
k = .5
tMax = 20
phi = 2
sampleRate = 1


dfDynamic = pd.DataFrame(columns=['id','val','Rolling_Average','Rolling_STD','bb_UP','bb_DOWN','sampleRateRate'])

def plot():
	global sampleRate
	sampleRate = 1
	df = pd.DataFrame(columns=['id','val'])
	chunksize = 1
	itr = 0
	for chunk in pd.read_csv('beachSampleData.csv', chunksize=chunksize):
		for index, row in chunk.head().iterrows():
			itr = itr + 1
			df.loc[itr] = [itr,row['Air Temperature']]
	
		

	df['Rolling_Average'] = df.iloc[:,1].rolling(window=windowSize).mean()
	df['Rolling_STD'] = df.iloc[:,1].rolling(window=windowSize).std()
	df['bb_UP'] = df.iloc[:,2] + (k*df.iloc[:,3])
	df['bb_DOWN'] = df.iloc[:,2] - (k*df.iloc[:,3])
	df['sampleRateRate'] = round((tMax)/(1+pow((2*k*df.iloc[:,3]),2)))
	print(df)

	ax = plt.gca()
	df.plot(kind='line',x='id',y='val',color='green',ax=ax,figsize=(15,10))
	df.plot(kind='line',x='id',y='Rolling_Average',color='black',ax=ax)
	df.plot(kind='line',x='id',y='bb_UP',color='blue',ax=ax)
	df.plot(kind='line',x='id',y='bb_DOWN',color='red',ax=ax)
	df.plot(kind='line',x='id',y='sampleRateRate',color='orange',ax=ax,alpha=0.5)
	
	plt.savefig('output.png')

def main():
	global sampleRate
	global dfDynamic
	df = pd.DataFrame(columns=['id','val'])
	chunksize = 1
	itr = 0
	#Iterate through raw input csv file (row by row)
	for chunk in pd.read_csv('beachSampleData.csv', chunksize=chunksize):
		for index, row in chunk.head().iterrows():
			itr = itr + 1
			# Only 'save' data based on sampleRate (default is 1 ie every value is saved)
			if(itr % sampleRate == 0):
				# Currently only saving an 'id' and 1 value 'Air Temp' (Needs to be adjusted for bigger examples)
				df.loc[itr] = [itr,row['Air Temperature']]
				# Keep a small DF as "rolling window"
				if(len(df.index) >= windowSize):
					# More robust df that calcuates meta data about rolling frame
					dfStats = df.copy()
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
					dfStats['sampleRateRate'] = sampleRate
					# Add new row to DF to keep track of total data (Used for diagram -- not needed in real life)
					dfDynamic =dfDynamic.append(dfStats.tail(1))
					df = df.drop(df.index[0])

			

	
	
	print ("Num of Rows = " + str(len(df.index)))
	print(dfDynamic)
	axDynamic = plt.gca()
	dfDynamic.plot(kind='line',x='id',y='val',color='green',ax=axDynamic,figsize=(25,10))
	dfDynamic.plot(kind='line',x='id',y='Rolling_Average',color='black',ax=axDynamic,figsize=(25,10))
	dfDynamic.plot(kind='line',x='id',y='bb_UP',color='blue',ax=axDynamic)
	dfDynamic.plot(kind='line',x='id',y='bb_DOWN',color='red',ax=axDynamic)
	dfDynamic.plot(kind='line',x='id',y='sampleRateRate',color='orange',ax=axDynamic,alpha=0.5)
	
	plt.savefig('outputTest.png')
	



	
if __name__ == '__main__':
    main()