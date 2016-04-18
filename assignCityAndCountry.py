from pyspark import SparkConf, SparkContext
from datetime import datetime,timedelta
from math import *
from operator import itemgetter

conf = (SparkConf()
         .setMaster("local")
         .setAppName("My app")
         .set("spark.executor.memory", "1g"))
sc = SparkContext(conf = conf)

print "\nSparkConf variables: ", conf.toDebugString()
print "\nSparkConf id: ", sc.applicationId
print "\nUser: ", sc.sparkUser()
print "\nVersion: ", sc.version

fs_with_zulu = sc.textFile("fs/TIST2015_ZULU/part-*", use_unicode=False)
data1 = sc.textFile("fs/dataset_TIST2015_Cities.txt")

header1 = data1.first()
data1 = data1.filter(lambda x:x !=header1)

#print(fs_with_zulu.count())

cities = data1.collect()
for i in range(len(cities)):
    cities[i] = cities[i].split("\t")
    cities[i][1] = float(cities[i][1])
    cities[i][2] = float(cities[i][2])

def haversine(lat0, lng0,lat1,lng1):
    lng0,lat0,lng1,lat1= map(radians, [lng0,lat0,lng1,lat1])

    dlng = lng1-lng0
    dlat = lat1-lat0
    a = sin(dlat/2)**2 + cos(lat0)*cos(lat1)*sin(dlng/2)**2
    c = 2*asin(sqrt(a))
    r = 6371
    return c*r

def map_city(data):
    temp=data.split("\t")
    distlist=[]
    for i in range(len(cities)):
        dist = haversine(float(temp[5]),float(temp[6]),cities[i][1], cities[i][2])
        distlist.append((i,dist))
    city = max(distlist,key=itemgetter(1))
    temp.append(cities[city[0]][0])
    temp.append(cities[city[0]][4])

    res = "\t".join(temp)
    return res
print("\n\n\n")
print("####################################")
print("\n\n\n")
fs_with_city = fs_with_zulu.map(map_city)
fs_with_city.saveAsTextFile("fs/TIST2015_CITY")
print("\n\n\n")
print("####################################")
print("\n\n\n")
#def f(x):print(x)
#fs_with_city.foreach(f)
