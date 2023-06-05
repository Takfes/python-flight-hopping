


# NEO4j basics
a = Node("Person", name="Alice", age=33)
b = Node("Person", name="Bob", age=44)
c = Node("Person", name="Helen", age=13)
KNOWS = Relationship.type("KNOWS")
graph.merge(KNOWS(a, b), "Person", "name")
graph.merge(c, "Person", "name")

# query by <id>
graph.nodes.get(914)

# query by name
graph.nodes.match("Person", name="Alice").first()
# graph.nodes.match("Person", name="Alice").limit(5)

len(graph.nodes)
graph.nodes

# query against neo4j
graph.run("MATCH (a:Person) RETURN a.name, a.age LIMIT 3").to_table()
graph.run("MATCH (a:Person) RETURN a.name, a.age").to_table()


# TOY DATASET
nodedata = [{'subid': '1', 'age': 75, 'fdg': 1.78, 'name': 'Mike'},
            {'subid': '2', 'age': 33, 'fdg': 1.56, 'name': 'June'},
            {'subid': '3', 'age': 32, 'fdg': 1.11, 'name': 'Jane'},
            {'subid': '4', 'age': 77, 'fdg': 1.02, 'name': 'Fred'},
            {'subid': '5', 'age': 26, 'fdg': 4.33, 'name': 'Alex'},
            {'subid': '6', 'age': 54, 'fdg': 2.11, 'name': 'Thom'},
            {'subid': '7', 'age': 24, 'fdg': 5.22, 'name': 'Codu'}]
nodes = pd.DataFrame(nodedata)

edgedata = [{'source': '1', 'dest': '2', 'weight': 1, 'rating': 2},
            {'source': '1', 'dest': '3', 'weight': 1, 'rating': 1},
            {'source': '1', 'dest': '5', 'weight': 1, 'rating': 6},
            {'source': '1', 'dest': '6', 'weight': 1, 'rating': 8},
            {'source': '6', 'dest': '7', 'weight': 1, 'rating': 3},
            {'source': '5', 'dest': '3', 'weight': 1, 'rating': 4},
            {'source': '4', 'dest': '3', 'weight': 1, 'rating': 9},
            {'source': '2', 'dest': '4', 'weight': 1, 'rating': 2}]
edges = pd.DataFrame(edgedata)

#cypher params
nodename = "Person"
edgename = "RATED"
source_name = "Person"
dest_name = "Person"

# iterate over a dataframe
for index, row in nodes.iterrows():
    node = Node(nodename, id=int(row['subid']), age = row['age'], fdg = row['fdg'], name = row['name'])
    graph.create(node)

for index, row in edges.iterrows():
    source_node = graph.match(nodename, property_key='id', property_value=int(row['source']))
    dest_node = graph.find_one(nodename, property_key='id', property_value = int(row['dest']))
    relation = Relationship(source_node, edgename, dest_node, rating = int(row['rating']))
    graph.create(relation)
