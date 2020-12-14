from networkx import *
import time

def parseData(filename):
  raw_data = open(filename)
  data = list(map(lambda x: x.split('\t'), raw_data))
  videoList = []
  for video in data:
    if(len(video)>1):
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
      videoList.append([videoID, uploader, int(age), category, int(length), int(views), float(rate), int(ratings), int(comments), relatedIDs, related_count])
  return videoList

def getEdges(videos):
  edges = []
  for video in videos:
    for related in video[9]:
      edges.append((video[0],related))
  return edges

graph = networkx.Graph()

videoVertex = parseData("4.txt")
videoEdges = getEdges(videoVertex)

vertextList = []
aggregation_time_start = time.time()
for video in videoVertex:
  vertextList.append(video[0])

graph.add_nodes_from(vertextList)

graph.add_edges_from(videoEdges)

aggregation_time_end = time.time()

print("Node count: ", graph.number_of_nodes())
print("Edge count: ", graph.number_of_edges())
# print("graph vertex: ", graph.nodes(5))
# print("graph edges: ", graph.edges(5))
# count = 0
# for v in graph.nodes():
#   print(v)
#   count += 1
#   if count > 10:
#     break

pagerank_time_start = time.time()
pagerank = pagerank(graph, max_iter = 20)
count = 0
max_rank = 0
max_page = str()
top_five = []

for key, value in pagerank.items():
    if value > max_rank:
        max_rank = value
        max_page = key
pagerank_time_end = time.time()

search_time_start = time.time()
ve = list(networkx.bfs_edges(graph, "MC--VwYTHAM"))
search_time_end = time.time()

print("Max Page: ", max_page, "Value: ", max_rank)

print("Aggregation time: ", aggregation_time_end - aggregation_time_start, "seconds")
print("Search time: ", search_time_end - search_time_start, "seconds")
print("Pagerank time: ", pagerank_time_end - pagerank_time_start ,"seconds")

count = 0
print(len(ve))
for v in ve:
  print(v)
  count += 1
  if count < 10:
    break