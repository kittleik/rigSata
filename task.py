from pyspark import SparkConf, SparkContext
from datetime import datetime,timedelta
from math import *
from operator import itemgetter

conf = (SparkConf()
         .setMaster("local[8]")
         .setAppName("My app")
         .set("spark.executor.memory", "8g"))
sc = SparkContext(conf = conf)

print "\nSparkConf variables: ", conf.toDebugString()
print "\nSparkConf id: ", sc.applicationId
print "\nUser: ", sc.sparkUser()
print "\nVersion: ", sc.version

data0 = sc.textFile("fs/dataset_TIST2015.tsv")
data1 = sc.textFile("fs/dataset_TIST2015_Cities.txt")

header1 = data1.first()
data1 = data1.filter(lambda x:x !=header1)
header = data0.first()
data0 = data0.filter(lambda x:x !=header)

cities = data1.collect()
for i in range(len(cities)):
    cities[i] = cities[i].split("\t")
    cities[i][1] = float(cities[i][1])
    cities[i][2] = float(cities[i][2])

def mapFunk(data):
    temp = data.split("\t")
    date = datetime.strptime(temp[3],'%Y-%m-%d %H:%M:%S')
    delta = int(temp[4])
    newTime = datetime.strftime(date + timedelta(minutes=delta), '%Y-%m-%d %H:%M:%S')
    temp[3] = newTime
    temp[4] = "0"
    res = "\t".join(temp)
    return res

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
    return "\t".join(temp)

#################################################################
print("\n\n\n")
print("#############################################################")
print("\n\n\n")
data_zulu = data0.map(mapFunk)
print (data_zulu.first())
print("\n\n\n")
print("#############################################################")
print("\n\n\n")
data_city = data_zulu.map(map_city)
print(data_city.first())
print("\n\n\n")
print("#############################################################")
print("\n\n\n")
unique_users = data0.countByKey().items()
print(len(unique_users))
