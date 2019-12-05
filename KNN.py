#!/usr/bin/env python
#import pandas as pd
from collections import OrderedDict
from operator import itemgetter
import math
import random
#import numpy as np

#local outlier factor
#If lof > 1 then outlier. If lof ~ 1 same density as neighbors. If lof < 1, high density.

class KNN:
        def __init__(self):
                self.k = 9 # can change this, but paper used 9
                self.data = []
	
        def add_number(self, val):
                self.data.append(val)

        #multivariate data
        def distance(self, a, b):
                total = 0
                for i in range(len(a)):
                        total += (a[i] - b[i])**2
                total = math.sqrt(total)
                return total

        def k_neighbors(self, a):
                neighbors = []
                for x in self.data:
                        neighbors.append([x,self.distance(x,a)])
                neighbors.sort(key=lambda x: x[1])
                neighbors = neighbors[0:self.k]
                return neighbors

        def k_distance(self, a):
                neighbors = self.k_neighbors(a)
                return neighbors[self.k-1][1]

        def reach_distance(self, a, b):
                return max(self.k_distance(b),self.distance(a,b))

        def local_reach_distance(self,a):
                neighbors = self.k_neighbors(a)
                total = 0
                for x in neighbors:
                        total += self.reach_distance(a,x[0])
                total = total / self.k
                lrd = 1 / total
                return lrd

        def local_outlier_factor(self,a):
                neighbors = self.k_neighbors(a)
                total = 0
                for x in neighbors:
                        total += self.local_reach_distance(x[0])
                total = total / self.k
                lof = total / self.local_reach_distance(a)
                return lof
                
        def outlier(self,val):
                if len(self.data) < self.k:
                        return False
                return self.local_outlier_factor(val) > 1
                        
'''
def main():
        print("test")
        knn = KNN()
        for i in range(25):
                rand1 = random.randint(0,100)
                rand2 = random.randint(0,100)
                knn.add_number([rand1,rand2])
        print(knn.outlier([20,20]))
        print(knn.outlier([50,50]))
        print(knn.outlier([30,30]))
        print(knn.outlier([10,10]))
        print(knn.outlier([0,0]))
'''
        

            
            
if __name__ == '__main__':
    main()




