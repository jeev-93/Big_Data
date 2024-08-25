import sys
from __future__ import division
from math import radians, cos, sin, asin, sqrt, exp
from datetime import datetime
from pyspark import SparkContext, StorageLevel

sc = SparkContext(appName = "Lab")
# sc.stop()
def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    km = 6367 * c
    return km

def d_date(date1,date2):
    """
    Calculate the distance between two day
    """
    date_diff=abs((date2 - date1).days)
    return date_diff

def d_time(time1,time2):
    """
    Calculate the distance between two time
    """
    time_diff=abs((time2 - time1).total_seconds() / 3600)
    return time_diff

def sum_kernel(d_distance,d_date,d_time):
    """
    Calculate the sum of the kernel
    """
    distance_kernel=exp(-(d_distance**2)/(h_distance**2))
    time_kernel=exp(-(d_time**2)/(h_time**2))
    date_kernel=exp(-(d_date**2)/(h_date**2))
    
    return distance_kernel+time_kernel+date_kernel

def prod_kernel(d_distance,d_date,d_time):
    """
    Calculate the prod of the kernel
    """
    distance_kernel=exp(-(d_distance**2)/(h_distance**2))
    time_kernel=exp(-(d_time**2)/(h_time**2))
    date_kernel=exp(-(d_date**2)/(h_date**2))
    
    return distance_kernel*time_kernel*date_kernel
    

h_distance = 480
h_date = 6
h_time = 3
a = 58.4274 # Up to you
b = 14.826 # Up to you
date = "2014-11-04" # Up to you
DATE_TIMESTAMP=datetime.strptime(date,"%Y-%m-%d")

stations = sc.textFile("BDA/input/stations.csv")
temps = sc.textFile("BDA/input/temperature-readings.csv")

stations_data=stations.map(lambda line: line.split(";")).map(lambda x:(str(x[0]),haversine(b,a,float(x[4]),float(x[3]))))
bc=sc.broadcast(stations_data.collectAsMap())    
joined_data = temps.sample(False, 0.1).map(lambda line: line.split(";"))\
                .map(lambda x:(str(x[0]),(datetime.strptime(x[1],"%Y-%m-%d"),datetime.strptime(x[2],"%H:%M:%S"),float(x[3]))))\
                .filter(lambda x: (x[1][0]<=DATE_TIMESTAMP))\
                .map(lambda x: (x[0],(bc.value[x[0]],d_date(x[1][0],DATE_TIMESTAMP),x[1][1],x[1][2])))
                
joined_data.cache()


#Sum of kernels
y_sum=[]

for time in ["0:00:00", "22:00:00", "20:00:00", "18:00:00", "16:00:00", "14:00:00",
"12:00:00", "10:00:00", "08:00:00", "06:00:00", "04:00:00"]:
    time=datetime.strptime(time,"%H:%M:%S")
    sum_kernel_rdd=joined_data.filter(lambda x:(x[1][1]>0 or (x[1][2] == 0 and  x[1][2]<time)))\
        .map(lambda x:(sum_kernel(x[1][0],x[1][1],d_time(time,x[1][2])),x[1][3]))
    sum_kernel_rdd=sum_kernel_rdd.map(lambda x:(x[0],x[0]*x[1])).reduce(lambda x,y:(x[0]+y[0],x[1]+y[1]))
    y_sum.append(sum_kernel_rdd[1]/sum_kernel_rdd[0])
    
time_list=["0:00:00", "22:00:00", "20:00:00", "18:00:00", "16:00:00", "14:00:00",
"12:00:00", "10:00:00", "08:00:00", "06:00:00", "04:00:00"]
dictionary = {k: v for k, v in zip(time_list, y_sum)}
dictionary_rdd = sc.parallelize(dictionary.items())
lines_rdd = dictionary_rdd.map(lambda x: "{}\t{}".format(x[0],x[1]))
lines_rdd.saveAsTextFile("BDA/output/predsum")

#Product of kernels
y_prod=[]

for time in ["0:00:00", "22:00:00", "20:00:00", "18:00:00", "16:00:00", "14:00:00",
"12:00:00", "10:00:00", "08:00:00", "06:00:00", "04:00:00"]:
    time=datetime.strptime(time,"%H:%M:%S")
    prod_kernel_rdd=joined_data.filter(lambda x:(x[1][1]>0 or (x[1][2] == 0 and  x[1][2]<time)))\
        .map(lambda x:(prod_kernel(x[1][0],x[1][1],d_time(time,x[1][2])),x[1][3]))
    prod_kernel_rdd=prod_kernel_rdd.map(lambda x:(x[0],x[0]*x[1])).reduce(lambda x,y:(x[0]+y[0],x[1]+y[1]))
    y_prod.append(prod_kernel_rdd[1]/prod_kernel_rdd[0])
time_list=["0:00:00", "22:00:00", "20:00:00", "18:00:00", "16:00:00", "14:00:00",
"12:00:00", "10:00:00", "08:00:00", "06:00:00", "04:00:00"]
dictionary = {k: v for k, v in zip(time_list, y_prod)}
dictionary_rdd = sc.parallelize(dictionary.items())
lines_rdd = dictionary_rdd.map(lambda x: "{}\t{}".format(x[0],x[1]))
lines_rdd.saveAsTextFile("BDA/output/predprod")

