#!/usr/bin/env python
import pandas as pd
import matplotlib.pyplot as plt
import math


windowSize = 35
k = .9
tMax = 20
sampleRate = 1
dfDynamic = pd.DataFrame(columns=['id','val','Rolling_Average','Rolling_STD','bb_UP','bb_DOWN','sampleRateRate'])
	

def main():
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
	df.plot(kind='line',x='id',y='val',color='green',ax=ax,figsize=(25,10))
	df.plot(kind='line',x='id',y='Rolling_Average',color='black',ax=ax,alpha=0.75, figsize=(15,10))
	df.plot(kind='line',x='id',y='bb_UP',color='blue',ax=ax,alpha=0.75)
	df.plot(kind='line',x='id',y='bb_DOWN',color='red',ax=ax,alpha=0.75)
	df.plot(kind='line',x='id',y='sampleRateRate',color='orange',ax=ax,alpha=0.5)
	
	plt.savefig('output.png')
	



	
if __name__ == '__main__':
    main()