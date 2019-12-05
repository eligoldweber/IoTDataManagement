#!/usr/bin/env python
import pandas as pd
from heapq import heappush, heappop

class MAD:
	def __init__(self):
		self.lower, self.upper = [], []
		self.diff = []

	def add_number(self, val):
		if not self.upper or val > self.upper[0]:
			heappush(self.upper, val)
		else:
			heappush(self.lower, -val)
		self.rebalance()

	def rebalance(self):
		if len(self.lower) - len(self.upper) > 1:
			heappush(self.upper, -heappop(self.lower))
		elif len(self.upper) - len(self.lower) > 1:
			heappush(self.lower, -heappop(self.upper))

	def get_median(self):
		if len(self.lower) == len(self.upper):
			return float((-self.lower[0] + self.upper[0]))/2.0
		elif len(self.lower) > len(self.upper):
			return float(-self.lower[0])
		else:
			return float(self.upper[0])

	def nlogn_median(self,l):
		l = sorted(l)
		if len(l) % 2 == 1:
			return l[len(l) / 2]
		else:
			return 0.5 * (l[len(l) / 2 - 1] + l[len(l) / 2])

	def chunked(self,l, chunk_size):
		return [l[i:i + chunk_size] for i in range(0, len(l), chunk_size)]

	def pick_pivot(self,l):
		assert len(l) > 0
		if len(l) < 5:
			return self.nlogn_median(l)
		chunks = self.chunked(l, 5)
		full_chunks = [chunk for chunk in chunks if len(chunk) == 5]
		sorted_groups = [sorted(chunk) for chunk in full_chunks]
		medians = [chunk[2] for chunk in sorted_groups]
		median_of_medians = self.quickselect_median(medians)
		return median_of_medians

	def quickselect(self,l, k):
		if len(l) == 1:
			assert k == 0
			return l[0]
		pivot = self.pick_pivot(l)
		lows = [el for el in l if el < pivot]
		highs = [el for el in l if el > pivot]
		pivots = [el for el in l if el == pivot]

		if k < len(lows):
			return self.quickselect(lows, k)
		elif k < len(lows) + len(pivots):
			return pivots[0]
		else:
			return self.quickselect(highs, k - len(lows) - len(pivots))

	def quickselect_median(self,l):
		if len(l) % 2 == 1:
			return self.quickselect(l, len(l) / 2)
		else:
			return 0.5 * (self.quickselect(l, len(l) / 2 - 1) + self.quickselect(l, len(l) / 2))

	def get_mad(self):
		del self.diff[:]
		median = self.get_median()
		for x in self.upper:
			self.diff.append(abs(x - median))
		for y in self.lower:
			self.diff.append(abs(-y - median))
		return self.quickselect_median(self.diff)

	def outlier(self, val):
		if not self.upper or not self.lower:
			return False
		median = self.get_median()
		mad = self.get_mad()
		lower_bound = median - (2.9652 * mad)
		upper_bound = median + (2.9652 * mad)
		return not(val >= lower_bound and val <= upper_bound)
		
# 5th and 95th percentile are outliers
# 2.9652 MADS from median is where outliers exist (2SD away)

#use class to filter out outliers

'''
def main():
	running_median = RunningStats()
	i = 1
	print([12, 4, 5, 3, 8, 7, 100, 8, 100])
	for n in (12, 4, 5, 3, 8, 7, 100, 8, 100):
		
		print("Index: " + str(i))
		if i > 1:
			print("Outlier? " + str(running_median.outlier(n)))
		if(not running_median.outlier(n)):
			running_median.add_number(n)
		print("Median: " + str(running_median.get_median()))
		print("MAD: " + str(running_median.get_mad()) + "\n")
		i += 1
'''	

            
            
if __name__ == '__main__':
    main()




