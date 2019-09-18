#!/usr/bin/env python

#Mapreduce implementation for K Means algorithm

import mincemeat
import random

points = [(0.72 ,0.44),
(0.16, 0.82),
(0.42, 0.37),
(0.19, 0.65)]

#get the closest centroid to a given point - v[0]-point, v[1]-centroids
def mapfn(k, v):
    closestCentroid = v[1][0]
    minDistance = 2
    for i in range(len(v[1])):
        d = (v[1][i][0] - v[0][0])**2 + (v[1][i][1] - v[0][1])**2
        if (d < minDistance):
            minDistance = d
            closestCentroid = v[1][i]
    yield closestCentroid, v[0]

#find the new mean of all the points that chose the same centroid
def reducefn(k, vs):
    points = vs  
    n = len(points)
    x = 0
    y = 0
    for i in range(n):
        x += points[i][0]
        y += points[i][1]
    return (x / n , y / n), points

def genRandom():
    return random.uniform(0.0 , 1.0)

def isImproved(res):
    for r in res:
        if r != res[r][0]:
            #print r,res[r][0]
            return True
    return False


k = 2
k_means =   [(genRandom() , genRandom()) for i in range(k+1)]

improved = True
i = 0
while improved:
    print i
    i+=1
    data = [(x, k_means) for x in points]

    s = mincemeat.Server()
    s.datasource = dict(enumerate(data))
    s.mapfn = mapfn
    s.reducefn = reducefn
    results = s.run_server(password="changeme")
    #print results

    k_means = [results[key][0] for key in results]
    improved = isImproved(results)

for key, val in results.items():
    print(key, " : ", val)