# Assignment 1
from pyspark import SparkContext
sc = SparkContext(appName = "Lab")

temp_reading = sc.textFile("BDA/input/temperature-readings.csv")
lines = temp_reading.map(lambda line: line.split(";"))

# (key, value) = (year,temperature)
year_temp = lines.map(lambda x: (x[1][0:4], float(x[3])))

#filter
year_temp = year_temp.filter(lambda x: int(x[0])>=1950 and int(x[0])<=2014)

#Get max
max_temp = year_temp.reduceByKey(lambda a,b: max(a, b))
max_temp = max_temp.sortBy(ascending = False, keyfunc=lambda k: k[1])

#Get min
min_temp = year_temp.reduceByKey(lambda a,b: min(a, b))
min_temp = min_temp.sortBy(ascending = False, keyfunc=lambda k: k[1])

# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
max_temp.saveAsTextFile("BDA/output/max")
min_temp.saveAsTextFile("BDA/output/min")

# Assigment 2.1

# (key, value) = ((year,month),temperature)
year_temp = lines.map(lambda x: ((x[1][0:4],x[1][5:7]), float(x[3])))

#filter
year_temp = year_temp.filter(lambda x: int(x[0][0])>=1950 and int(x[0][0])<=2014 and x[1]>10)

#(key, value) = ((year,month),count)
year_month_count=year_temp.map(lambda x:(x[0],1))

#Get counts
year_month_counts = year_month_count.reduceByKey(lambda a,b: a+1)

# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
year_month_counts.saveAsTextFile("BDA/output")

# Assigment 2.2
# (key, value) = ((year,month,station),temperature)
year_temp = lines.map(lambda x: ((x[1][0:4],x[1][5:7],x[0]), float(x[3])))\
                    .filter(lambda x: int(x[0][0])>=1950 and int(x[0][0])<=2014 and x[1]>10)

# Reduce the value
year_month_sta_count=year_temp.reduceByKey(lambda x,y:max(x,y))\
                    .map(lambda x:((x[0][0],x[0][1]),x[1]))\
                    .countByKey()         #Count the number of each key and return a dict

sc.parallelize(list(year_month_sta_count.items())).saveAsTextFile("BDA/output")

# Assigment 3
# (key, value) = ((year,month,date,station),temperature)
year_temp = lines.map(lambda x: ((x[1][0:4],x[1][5:7],x[1][8:],x[0]), float(x[3])))\
                    .filter(lambda x: int(x[0][0])>=1960 and int(x[0][0])<=2014)

# Compute the month average temperature for each station
#daily max temperature
daily_max_temp=year_temp.reduceByKey(lambda x,y:max(x,y))
#daily min temperature
daily_min_temp=year_temp.reduceByKey(lambda x,y:min(x,y))
#Average monthly temperature
avg_monthly_temp=daily_max_temp.join(daily_min_temp)\
                        .map(lambda x:((x[0][0],x[0][1],x[0][3]),(x[1][0]+x[1][1])/2))\
                        .groupByKey()\
                        .mapValues(lambda x:sum(x)/len(x))


print(avg_monthly_temp.collect())

# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
avg_monthly_temp.saveAsTextFile("BDA/output")

# Assigment 4
temp_reading = sc.textFile("BDA/input/temperature-readings.csv")
precipitation_file=sc.textFile("BDA/input/precipitation-readings.csv")
temperature_lines = temp_reading.map(lambda line: line.split(";"))
precipitation_lines = precipitation_file.map(lambda line: line.split(";"))

#Temperature
# (key, value) = (station,temperature)
station_temperature = temperature_lines.map(lambda x: (x[0], float(x[3])))\
                    .reduceByKey(lambda x,y:max(x,y))\
                    .filter(lambda x:x[1]>25 and x[1]<30)

#Precipitation
# (key, value) = ((year,month,date,station),precipitation)
station_precipitation= precipitation_lines.map(lambda x: ((x[1][0:4],x[1][5:7],x[1][8:],x[0]), float(x[3])))\
                    .reduceByKey(lambda x,y:x+y)\
                    .map(lambda x:(x[0][3],x[1]))\
                    .reduceByKey(lambda x,y:max(x,y))\
                    .filter(lambda x:x[1]>100 and x[1]<200)

#Join
conbine_data=station_temperature.join(station_precipitation)

# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
conbine_data.saveAsTextFile("BDA/output")

# Assigment 5
precipitation_file=sc.textFile("BDA/input/precipitation-readings.csv")
ost_station_file=sc.textFile("BDA/input/stations-Ostergotland.csv")
precipitation_lines = precipitation_file.map(lambda line: line.split(";"))
ost_station_lines_bc=ost_station_file.map(lambda line: line.split(";")).map(lambda x:int(x[0]))

# broadcast
bc=sc.broadcast(ost_station_lines_bc.collect())

# (key, value) = ((year,month,station),precipitation)
ost_average_monthly_precipitation=precipitation_lines.filter(lambda x:(int(x[0])in bc.value) and \
                                    int(x[1][0:4])>=1993 and int(x[1][0:4])<=2016)\
        .map(lambda x:((x[1][0:4],x[1][5:7],x[0]),float(x[3])))\
        .reduceByKey(lambda x,y:x+y)\
        .map(lambda x:((x[0][0],x[0][1]),x[1]))\
        .groupByKey()\
        .mapValues(lambda x:sum(x)/len(x))

print(ost_average_monthly_precipitation.collect())

# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
ost_average_monthly_precipitation.saveAsTextFile("BDA/output")

