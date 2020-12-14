from functools import reduce
from pyspark.sql.functions import col, lit, when
from pyspark import SparkContext
from pyspark.sql import SQLContext


sc =SparkContext()
sc.addPyFile('./spark-3.0.1-bin-hadoop2.7/jars/graphframes-0.8.1-spark3.0-s_2.12.jar')
sqlContext = SQLContext(sc)
from pyspark.sql.session import SparkSession
spark = SparkSession(sc)

from pyspark.sql.types import *
from graphframes import *
import timeit
import time

def createVertices(datafile):
    raw_data = sc.textFile(datafile)
    data = raw_data.map(lambda x:x.split("\t"))
    videos = data.take(data.count())
    rdd_videos = sc.parallelize(videos)
    #videoList = rdd_videos.map(lambda x: (x[0],x[1],x[2],x[3],x[4],x[5],x[6],x[7],x[8],x[9:]) if len(x) > 1 else None).collect()
    vertexList = []
    for video in videos:
        if len(video) > 1:
            videoID = video[0]
            uploader = video[1]
            age = video[2]
            category = video[3]
            length = video[4]
            views = video[5]
            rate = video[6]
            ratings = video[7]
            comments = video[8]
            relatedIDs = []
            if len(video) > 9:
                relatedIDs = video[9:]
            related_count = len(relatedIDs)
            vertexList.append((videoID, uploader, int(age), category, int(length), int(views), float(rate), int(ratings), int(comments), relatedIDs, related_count))
    return vertexList

def createEdges(vertices):
    rdd_edges = sc.parallelize(vertices)
    edges = rdd_edges.map(lambda video: list(map(lambda x: (video[0], x, "related"), video[9]))).collect()
    finalEdges = []
    for index in edges:
        for edge in index:
            finalEdges.append(edge)
    return finalEdges
f __name__=="__main__":
    filenames = ["./data/0222/0.txt","./data/0222/1.txt","./data/0222/2.txt","./data/0222/3.txt"]
    start1 = timeit.timeit()

    vertices = createVertices("./data/0222/4.txt")

    vList = []
    i = 0
    while i < 350000:
        vList.append(vertices[i])
        i += 1

    edges = createEdges(vList)
    print("VERTICES")
    for i in vertices[:4]:
        print(i)
    print("EDGES")
    for i in edges[:4]:
        print(i)

    v = sqlContext.createDataFrame(vList, ["id", "uploader", "age","category", "length", "views", "rate", "ratings","comments","relatedIDs","related_count"])
    e = sqlContext.createDataFrame(edges,["src","dst","relation"])

    end = timeit.timeit()
    print()
    print("TIME TO GET DATA=", end - start1)
    print()

    start = time.time()
    g = GraphFrame(v,e)
    end = time.time()
    print()
    print("TIME TO MAKE GRAPH=",end-start)
    print()

start = time.time()
    in_Degrees = g.inDegrees.orderBy("inDegree", ascending=False)
    in_Degrees.show()
    g.outDegrees.orderBy("outDegree",ascending=False).show()
    end = time.time()
    print()
    print("TIME TO GET DEGREES=",end-start)
    print()

    start = time.time()
    count = g.edges.filter("relation = 'related'").count()

    total_degree = count * 2
    avg_degree = total_degree / 350000
    end = time.time()
    print()
    print("TIME TO COUNT RELATIONS=", end-start)
    print()
    print("TOTAL TIME:",end-start1)
    print()
    end = time.time()
    print()
    print("TIME TO GET DATA=", end - start)
    print()

    start = time.time()
    results = g.pageRank(resetProbability=0.1,maxIter=5)
    end = time.time()

    time_search_start = time.time()
    max_degree = g.vertices.groupBy().max("related_count")
    min_degree = g.vertices.groupBy().min("related_count")
    max_size = g.vertices.groupBy().max("length")
    max_views= g.vertices.groupBy().max("views")
    time_search_end = time.time()

category_music = g.vertices.filter("category = 'Music'").count()
    category_news = g.vertices.filter("category = 'News & Politics'").count()
    category_ent = g.vertices.filter("category = 'Entertainment'").count()
    category_sports = g.vertices.filter("category = 'Sports'").count()
    category_games = g.vertices.filter("category = 'Gadgets & Games'").count()
    category_comedy = g.vertices.filter("category = 'Comedy'").count()
    category_people = g.vertices.filter("category = 'People & Blogs'").count()
    category_film = g.vertices.filter("category = 'Film & Animation'").count()
    category_places = g.vertices.filter("category = 'Travel & Places'").count()
    entertainment_cat = g.vertices.filter("category = 'Entertainment'")
    import pyspark.sql.functions as f
    entertainment_len = entertainment_cat.filter((f.col('length') > 100))
    category_music = g.vertices.filter("category = 'Music'")
    category_news = g.vertices.filter("category = 'News & Politics'")
    category_ent = g.vertices.filter("category = 'Entertainment'")
    category_sports = g.vertices.filter("category = 'Sports'")
    category_games = g.vertices.filter("category = 'Gadgets & Games'")
    category_comedy = g.vertices.filter("category = 'Comedy'")
    category_people = g.vertices.filter("category = 'People & Blogs'")
    category_film = g.vertices.filter("category = 'Film & Animation'")
    category_places = g.vertices.filter("category = 'Travel & Places'")


    print()
    print("***************** NETWORK AGGREGATION ***************")

    print()
    print("RELATION COUNT =", count)
    print("AVERAGE DEGREE =", avg_degree)
    print("MIN DEGREE = ", 0)

    get_degree_start = time.time()
    max_degree.show()
    min_degree.show()
    max_size.show()
    max_views.show()
                                              
print("*****************************************************")

    print()

    print("******************* SEARCH **************************")
    print()

    print("Categories")
    print("Music =",category_music)
    print("News & Politics =", category_news)
    print("Entertainment =", category_ent)
    print("Sports =", category_sports)
    print("Gadget & Games =", category_games)
    print("Comedy =", category_people)
    print("People & Blogs =", category_people)
    print("Film & Animation = ", category_film)
    print("Travel & Places = ", category_places)

    category_music.show()
    category_news.show()
    category_ent.show()
    category_sports.show()
    category_games.show()
    category_comedy.show()
    category_people.show()
    category_film.show()
    category_places.show()


    entertainment_len.show()
     print()
    print("******************* INFLUENCE ************************")
    results.vertices.orderBy("pagerank", ascending=False).show()

    print()
    print("TIME FOR PAGERANK =", end - start)
    print()
    print("TIME FOR Aggregation =", get_degree_end - get_degree_start)
    print()
    print("TIME FOR SEARCH =", time_search_end - time_search_start)
    print()

    

