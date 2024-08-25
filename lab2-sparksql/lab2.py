from pyspark import SparkContext
from pyspark.sql import SQLContext, Row, SparkSession
from pyspark.sql import functions as F

sc = SparkContext(appName="BDALab2")
sqlContext = SQLContext(sc)

temp_reading = sc.textFile("BDA/input/temperature-readings.csv")
lines = temp_reading.map(lambda line: line.split(";"))

# (key, value) = (year,temperature)
yearTemp_reading = lines.map(lambda p: Row(station=p[0],
date=p[1], year=p[1].split("-")[0],time=p[2],value=float(p[3]),quality=p[4]))

schemayearTemp_reading = sqlContext.createDataFrame(yearTemp_reading)
schemayearTemp_reading.registerTempTable("yearTemp_reading")


minYearTempSta = schemayearTemp_reading.where((schemayearTemp_reading["year"] >= 1950) &
(schemayearTemp_reading["year"] <= 2014)). \
groupBy('year', 'station'). \
agg(F.min('value').alias('annualMin')). \
select('year', 'station', 'annualMin')
# orderBy(['annualMin'], ascending=[False])

TempMinByYear = schemayearTemp_reading.groupBy('year'). \
agg(F.min('value').alias('annualMin'))

TempMinByYearWithStation = minYearTempSta.join(TempMinByYear, on=['year', 'annualMin'])\
.select('year', 'station', 'annualMin')\
.orderBy(['annualMin'], ascending=[False])

# ------max
maxYearTempSta = schemayearTemp_reading.where((schemayearTemp_reading["year"] >= 1950) &
(schemayearTemp_reading["year"] <= 2014)). \
groupBy('year', 'station'). \
agg(F.max('value').alias('annualMax')). \
select('year', 'station', 'annualMax')
# orderBy(['annualMax'], ascending=[False])

TempMaxByYear = schemayearTemp_reading.groupBy('year'). \
agg(F.max('value').alias('annualMax'))

TempMaxByYearWithStation = maxYearTempSta.join(TempMaxByYear, on=['year', 'annualMax'])\
.select('year', 'station', 'annualMax')\
.orderBy(['annualMax'], ascending=[False])

# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
TempMaxByYearWithStation.rdd.saveAsTextFile("BDA/output/max")
TempMinByYearWithStation.rdd.saveAsTextFile("BDA/output/min")

temp_reading = sc.textFile("BDA/input/temperature-readings.csv")
lines = temp_reading.map(lambda line: line.split(";"))

yearTemp_reading = lines.map(lambda p: Row(station=p[0],
                                       date=p[1],
                                       year=p[1].split("-")[0],
                                       month=p[1].split("-")[1],
                                       time=p[2],
                                       value=float(p[3]),
                                       quality=p[4]))

schemayearTemp_reading = sqlContext.createDataFrame(yearTemp_reading)
# schemayearTemp_reading.registerTempTable("yearTemp_reading")


# part 1
temperature_count = schemayearTemp_reading.where(
    (schemayearTemp_reading["year"] >= 1950) & (schemayearTemp_reading["year"] <= 2014) &
    (schemayearTemp_reading["value"] >= 10)) \
    .select('year', 'month') \
    .groupBy('year', 'month') \
    .count() \
    .orderBy('count', ascending=[False])

# part 2
temperature_countFromStation = schemayearTemp_reading.where(
    (schemayearTemp_reading["year"] >= 1950) & (schemayearTemp_reading["year"] <= 2014) &
    (schemayearTemp_reading["value"] >= 10)) \
    .select('year', 'month', 'station') \
    .groupBy('year', 'month', 'station') \
    .count() \
    .groupBy('year', 'month').count() \
    .orderBy('count', ascending=[False])

# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
temperature_count.rdd.saveAsTextFile("BDA/output/1")
temperature_countFromStation.rdd.saveAsTextFile("BDA/output/2")

temp_reading = sc.textFile("BDA/input/temperature-readings.csv")
lines = temp_reading.map(lambda line: line.split(";"))

yearTemp_reading = lines.map(lambda p: Row(station=p[0],
                                       date=p[1],
                                       year=p[1].split("-")[0],
                                       month=p[1].split("-")[1],
                                       time=p[2],
                                       value=float(p[3]),
                                       quality=p[4]))

schemayearTemp_reading = sqlContext.createDataFrame(yearTemp_reading)
# schemayearTemp_reading.registerTempTable("yearTemp_reading")

DailyTemperature = schemayearTemp_reading.where((schemayearTemp_reading["year"] >= 1950) & (schemayearTemp_reading["year"] <= 2014)) \
    .groupBy('year', 'month', 'date', 'station') \
    .agg(F.min('value').alias('min'), F.max('value').alias('max'))

avgDailyTemperature = DailyTemperature.withColumn('avgDailyTemperature',
                                                  (DailyTemperature['min'] + DailyTemperature['max']) * 0.5)

avgMonthlyTemperature = avgDailyTemperature.select('year', 'month', 'station', 'avgDailyTemperature') \
    .groupBy('year', 'month', 'station') \
    .agg(F.avg('avgDailyTemperature').alias('avgMonthlyTemperature')) \
    .orderBy('avgMonthlyTemperature', ascending=[False])

# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
avgMonthlyTemperature.rdd.saveAsTextFile("BDA/output/A2Q3")

temp_reading = sc.textFile("BDA/input/temperature-readings.csv")
precipitation_file = sc.textFile("BDA/input/precipitation-readings.csv")
# ---
lines = temp_reading.map(lambda line: line.split(";"))

yearTemp_reading = lines.map(lambda p: Row(station=p[0],
                                       date=p[1],
                                       year=p[1].split("-")[0],
                                       month=p[1].split("-")[1],
                                       time=p[2],
                                       value=float(p[3]),
                                       quality=p[4]))

schemayearTemp_reading = sqlContext.createDataFrame(yearTemp_reading)
# schemayearTemp_reading.registerTempTable("yearTemp_reading")

# -----
lines = precipitation_file.map(lambda line: line.split(";"))

precipReadings = lines.map(lambda p: Row(station=p[0],
                                         date=p[1],
                                         year=p[1].split("-")[0],
                                         month=p[1].split("-")[1],
                                         time=p[2],
                                         value=float(p[3]),
                                         quality=p[4]))

schemaPrecipReadings = sqlContext.createDataFrame(precipReadings)

# ----
# schemayearTemp_reading.show()
# schemaPrecipReadings.show()

MaxTemp = schemayearTemp_reading.groupBy('station')\
    .agg(F.max('value').alias('maxTemp'))

MaxTemp = MaxTemp.where((MaxTemp['maxTemp'] >=25) & (MaxTemp['maxTemp'] <=30))

MaxPrecip = schemaPrecipReadings.groupBy('station', 'date')\
    .agg(F.sum('value').alias('totDailyPrecipitation'))\
    .groupBy('station').agg(F.max('totDailyPrecipitation').alias('maxDailyPrecipitation'))

MaxPrecip = MaxPrecip.where( (MaxPrecip['maxDailyPrecipitation'] >= 100) &
(MaxPrecip['maxDailyPrecipitation'] <= 200))

stationList = MaxTemp.join(MaxPrecip, on = ['station'])\
    .select('station', 'maxTemp', 'maxDailyPrecipitation')\
    .orderBy('station', ascending=[False])

# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
stationList.rdd.saveAsTextFile("BDA/output")

precipitation_file = sc.textFile("BDA/input/precipitation-readings.csv")
lines = precipitation_file.map(lambda line: line.split(";"))

precipReadings = lines.map(lambda p: Row(station=p[0],
                      date=p[1],
                      year=p[1].split("-")[0],
                      month=p[1].split("-")[1],
                      time=p[2],
                      value=float(p[3]),
                      quality=p[4]))

schemaPrecipReadings = sqlContext.createDataFrame(precipReadings)

# -----
station_file = sc.textFile("BDA/input/stations-Ostergotland.csv")
lines = station_file.map(lambda line: line.split(";"))

StationReadings = lines.map(lambda p: Row(station=p[0]))

schemaStationReadings = sqlContext.createDataFrame(StationReadings)

# ----
# schemaPrecipReadings.show()
# schemaStationReadings.show()

schemaPrecipReadings = schemaPrecipReadings.join(schemaStationReadings, on = ['station'])

schemaPrecipReadings = schemaPrecipReadings.where((schemaPrecipReadings["year"] >= 1993) &
(schemaPrecipReadings["year"] <= 2016))\
    .groupBy("year", 'month', 'station').agg(F.sum('value').alias('totMonPrecip'))\
    .orderBy(["year", 'month'], ascending=[False, False])\
    .groupBy("year", 'month').agg(F.avg('totMonPrecip').alias('avgMonthlyPrecipitation'))


# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
schemaPrecipReadings.rdd.saveAsTextFile("BDA/output")