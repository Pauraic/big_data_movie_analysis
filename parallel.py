testing = 0
from pyspark import SparkContext,SparkConf
import ast

conf = SparkConf() \
            .setAppName("MovieAnalysis") \
                .setMaster("local[*]") \
                    .set("spark.executor.memory", "4g") \
                    .set("spark.driver.memory", "4g")    


sc = SparkContext(conf=conf)


#rdd
credits = sc.textFile("tmdb_5000_credits.csv")
box_office = sc.textFile("boxoffice_data_2024.csv")


def clean_numberi_split(line):
    entries = line.split(',')
    val = entries[-1].replace('$','').replace(',','')
    entries[-1]=val
    return entries

def split(line):
    entries=line.split(',')
    return entries

credits = credits.map(split)
box_office = box_office.map(clean_numberi_split)


def parse_rwo(row):
    cast = ast.literal_eval(row[2])
    crew = ast.literal_eval(row[3])

#get dirs and actors
    directors = [person["name"] for person in crew if person["job"] == "Director"]
    actors = [person["name"] for person in cast]

    return [(director,actor,row[0]) for director in directors for actor in actors]

pairs = credits.flatMap(parse_rwo)

def keepcol1and4(row):
    try:
        return (row[0],float(row[3]))
    except ValueError:
        return (row[0],0.0)

box_office = box_office.map(keepcol1and4)

#join
joined = pairs.keyBy(lambda x: x[2]).join(box_office.keyBy(lambda x: x[0]))

#unpack the join data
def unpack(data):
    (director,actor,title), (ignore,revenue) = data
    return (director,actor,revenue)

ungrouped = joined.map(unpack)


ungroupedfiltered = ungrouped.filter(lambda x: x[2] is not None and x[2] != 0)

result = ungroupedfiltered.repartition(1)\
        .map(lambda x: ((x[0],x[1]),x[2]))\
        .reduceByKey(lambda a,b:a+b)\
        .map(lambda x: (x[0][0],x[0][1],x[1]))\
        .sortBy(lambda x: x[2], ascending=False)

top30 = result.take(30)
for row in top30:
    print (row)


sc.stop()
