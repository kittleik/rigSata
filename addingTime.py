from pyspark import SparkConf, SparkContext
from datetime import datetime,timedelta

conf = (SparkConf()
         .setMaster("local")
         .setAppName("My app")
         .set("spark.executor.memory", "1g"))
sc = SparkContext(conf = conf)

data0 = sc.textFile("fs/dataset_TIST2015.tsv")
data1 = sc.textFile("fs/resources/dataset_TIST2015_Cities.txt")

data0.count()
#data1.count()

def mapFunk(data):
    temp = data.split("\t")
    date = datetime.strptime(temp[3],'%Y-%m-%d %H:%M:%S')
    delta = int(temp[4])
    newTime = datetime.strftime(date + timedelta(minutes=delta), '%Y-%m-%d %H:%M:%S')
    temp[3] = newTime
    temp[4] = "0"
    res = "\t".join(temp)
    return res


header = data0.first()
data0 = data0.filter(lambda x:x !=header)
#data0.foreach(funk)
newTimeRDD = data0.map(mapFunk)
newTimeRDD.count()
