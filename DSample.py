#!/usr/bin/env python
import pandas as pd
import matplotlib.pyplot as plt
import math


class DSample:
	def __init__(self,sample,startK,max,window,p):
		self.sampleRate = sample
		self.k = startK
		self.tMax = max
		self.windowSize = window
		self.phi = p
		self.rawCount = 0
		self.uid = 0
		self.dfRaw = pd.DataFrame(columns=['id','val'])

	def add_val(self, id,val):
		if(self.rawCount % self.sampleRate == 0):
			self.dfRaw.loc[self.rawCount] = [self.rawCount,val]
			if(len(self.dfRaw.index) >= self.windowSize):
				# More robust df that calcuates meta data about rolling frame
				dfStats = self.dfRaw.copy()
				dfStats['Rolling_Average'] = dfStats.iloc[:,1].rolling(window=self.windowSize).mean()
				dfStats['Rolling_STD'] = dfStats.iloc[:,1].rolling(window=self.windowSize).std()
				dfStats['bb_UP'] = dfStats.iloc[:,2] + (self.k*dfStats.iloc[:,3])
				dfStats['bb_DOWN'] = dfStats.iloc[:,2] - (self.k*dfStats.iloc[:,3])

				std = float(dfStats.tail(1)['Rolling_STD'])
				up = float(dfStats.tail(1)['bb_UP'])
				down = float(dfStats.tail(1)['bb_DOWN'])
				dyn = round(2*self.k*std,4)
				
				assert dyn == round(abs(up - down),4)
				rawsampleRate = (self.tMax)/(1+pow(dyn,self.phi))
				# Cant sample less than every data point, so rouind up
				if(rawsampleRate < 1):
					self.sampleRate = math.ceil(rawsampleRate)
				else:
					self.sampleRate = round(rawsampleRate)
				dfStats['sampleRate'] = self.sampleRate
				self.dfRaw = self.dfRaw.drop(self.dfRaw.index[0])
				tuplePoint =(1,self.uid,val)
				# data.uid = uidCnt
				self.uid = self.uid + 1
				self.rawCount = self.rawCount + 1;
				return tuplePoint
			else:
				tuplePoint =(1,self.uid,val)
				self.uid = self.uid +1
				self.rawCount = self.rawCount + 1;
				return tuplePoint
				
		self.rawCount = self.rawCount + 1;
		return (0,self.rawCount,val)


# def main():
# 	sample = DSample(1,.9,20,5,2)
# 	print(sample.tMax)
# 	print(sample.add_val(1,5))
# 	print(sample.add_val(1,5))
# 	print(sample.add_val(1,5))
# 	print(sample.add_val(1,5))
# 	print(sample.add_val(1,5888))
# 	print(sample.add_val(1,5))
# 	print(sample.add_val(1,5))
# 	print(sample.add_val(1,5))
# 	print(sample.add_val(1,5888))

            
            
if __name__ == '__main__':
    main()