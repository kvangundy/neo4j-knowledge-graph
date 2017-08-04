#ABM --> CliqueBM
from py2neo import Graph
from igraph import Graph as IGraph
import wikipedia
import re

graph = Graph()

#you can substitue any list of companies here, just make sure the first column is the name of the company
companyListURL = 'https://raw.githubusercontent.com/kvangundy/neo4j-knowledge-graph/master/data/global_2k_list.csv'

#cypher queries 

global2klist = 'LOAD CSV FROM "' + companyListURL + '" AS line WITH trim(line[0]) as line RETURN *'

emptyDB='''
MATCH (n)
DETACH DELETE n;
'''

createCompany = '''
MERGE (c:Company {id:{companyName}})
ON CREATE SET c.context = {wikiResp}, c.bioSplit = FALSE;
'''

parseBio = '''
CALL apoc.periodic.iterate(\'MATCH(p:Company) WHERE p.bioSplit = FALSE RETURN p\', 
	\'WITH p, split(p.context, \" \") as words UNWIND words as word MERGE (n:Tag {tag:word}) MERGE (p)-[r:HAS_TAG]->(n) ON MATCH SET r.count = r.count + 1 REMOVE p.bioSplit\'
	, {batchSize:100, parallel:true})
'''

findSimilar = '''
MATCH (p:Company)-[:HAS_TAG]->()<-[:HAS_TAG]-(other:Company)
WHERE ID(p) < ID(other)
RETURN p.id, other.id, COUNT(*) AS weight;
'''

createCluster = '''
UNWIND {params} AS p 
MATCH (t:Company {id: p.id}) 
MERGE (cluster:Cluster {name: p.group})
MERGE (t)-[:IN_CLUSTER]->(cluster);
'''

#list of neo4j properties to be indexed
cypher_createIndexesList = [':Company(id)']

try:
	nukeDB = raw_input('''Would you like to empty your database? type \'empty database\' otherwise, press return to proceed with current data\n''').lower()
			
	if nukeDB == 'empty database':
		print('Clearing database... \n')
		graph.cypher.execute(emptyDB)
		print('Database cleared... \n')
		
except Exception as e:
	print('couldn\'t nuke the db... \n')
	print(e)

#loading companies listed in CSV, enriching them with their wikipedia summary, then loading those nodes into Neo4j
try:
	print('Loading data... \n')

	for x in cypher_createIndexesList:
		graph.cypher.execute('CREATE INDEX ON ' + x + ';')

	companyNames = graph.cypher.execute(global2klist)

	for row in companyNames:
		try:
			companyName = str(row["line"])
		except Exception as e:
			print(e)
			continue

		try:
			print(companyName)
			wikiResp = re.sub('[^A-Za-z0-9 -]', '', wikipedia.summary(row)).lower() #contains regex to strip all non A-Z 0-9 characters
			graph.cypher.execute(createCompany, parameters = {"companyName" : companyName, "wikiResp" : wikiResp})
		except Exception as e:
			print(e)
			continue

except Exception as e:
	print(e)

#using iGraph to detect densely connected subgraphs
try:
	graph.cypher.execute(parseBio)

	#walktrap is a function tries to find densely connected subgraphs, also called communities in a graph via random walks. 
	#The idea is that short random walks tend to stay in the same community.
		
	ig = IGraph.TupleList(graph.cypher.execute(findSimilar), weights=True)
	clusters = IGraph.community_walktrap(ig, weights="weight")
	clusters = clusters.as_clustering()

	nodes = [node["name"] for node in ig.vs]
	nodes = [{"id": x, "label": x} for x in nodes]

	for node in nodes:
		idx = ig.vs.find(name=node["id"]).index
		node["group"] = clusters.membership[idx]

	count = str(len(clusters))
	print(count +' clusters found ' '\n')

	#write it back into Neo4j
	graph.cypher.execute(createCluster, params = nodes)

	print("All clusters added to Neo4j \n")

except Exception as e:
	print('the error returned : \n')
	print(e)
