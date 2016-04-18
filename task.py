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

def map_init(data):
    temp = data.split("\t")
    temp[0] = int(temp[0])
    temp[1] = int(temp[1])
    temp[2] = str(temp[2])
    temp[3] = datetime.strptime(temp[3],'%Y-%m-%d %H:%M:%S')
    temp[4] = int(temp[4])
    temp[5] = float(temp[5])
    temp[6] = float(temp[6])
    temp[7] = str(temp[7])
    temp[8] = str(temp[8])
    return  temp

def map_zulu_time(data):
    temp = data[:]
    date = temp[3]
    delta = temp[4]
    newTime = datetime.strftime(date + timedelta(minutes=delta), '%Y-%m-%d %H:%M:%S')
    temp[3] = newTime
    return temp

def haversine(lat0, lng0,lat1,lng1):
    lng0,lat0,lng1,lat1= map(radians, [lng0,lat0,lng1,lat1])

    dlng = lng1-lng0
    dlat = lat1-lat0
    a = sin(dlat/2)**2 + cos(lat0)*cos(lat1)*sin(dlng/2)**2
    c = 2*asin(sqrt(a))
    r = 6371
    return c*r

def map_city(data):
    temp = data[:]
    distlist=[]
    for i in range(len(cities)):
        dist = haversine(float(temp[5]),float(temp[6]),cities[i][1], cities[i][2])
        distlist.append((i,dist))
    city = max(distlist,key=itemgetter(1))
    temp.append(str(cities[city[0]][0]))
    temp.append(str(cities[city[0]][4]))
    return temp

def map_key_value_id(data):
    return (data[1],1)

def map_key_value_session(data):
    return (data[2],1)

def map_key_value_country(data):
    return (data[10],1)

def map_key_value_city(data):
    return (data[9],1)

#################################################################
print("\n\n\n")
print("#############################################################")
print("\n\n\n")
data_init = data0.map(map_init)
print(data_init.first())
print("\n\n\n")
print("#############################################################")
print("\n\n\n")
data_zulu = data_init.map(map_zulu_time)
print (data_zulu.first())
print("\n\n\n")
print("#############################################################")
print("\n\n\n")
data_city = data_zulu.map(map_city)
print(data_city.first())
print("\n\n\n")
print("#############################################################")
print("\n\n\n")

"""
-----4a------
key_value_id = data_init.map(map_key_value_id)
unique_users =key_value_id.countByKey().items()
print(len(unique_users))

#>> 256307

print("\n\n\n")
print("#############################################################")
print("\n\n\n")

#------4b-------

print(data0.count())

#>>19265256

print("\n\n\n")
print("#############################################################")
print("\n\n\n")

#------4c-------
key_value_session = data_init.map(map_key_value_session)
unique_sessions = key_value_session.countByKey().items()
print(len(unique_sessions))

#>>6338302

print("\n\n\n")
print("#############################################################")
print("\n\n\n")
"""

#--------------4d----------
key_value_country = data_city.map(map_key_value_country)
unique_countries = key_value_country.countByKey().items()

#--------4e---------------
#key_value_city = data_city.map(map_key_value_city)
#unique_cities = key_value_city.countByKey().items()

print(len(unique_countries))
print(len(unique_cities))
print("\n\n\n")
print("#############################################################")
print("\n\n\n")
