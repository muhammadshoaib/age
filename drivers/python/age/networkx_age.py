'''
Author - Tito Osadebey
Github - https://github.com/titoausten
Email - osadebe.tito@gmail.com
'''
import age
import networkx as nx

GRAPH_NAME = "teest"
DSN = "host=localhost port=5432 dbname=tee_demodb user=tito password=agens"

ag = age.connect(graph=GRAPH_NAME, dsn=DSN)

#   LOAD TO NETWORKX (From Cypher query output)
def load_to_networkx(cursor) -> nx.DiGraph:
    graph = nx.DiGraph()

    cypher_query_output = cursor.fetchall()
    for row in cypher_query_output:
        item = row[0]
        if isinstance(item, age.models.Path):
            for entity in item:
                if isinstance(entity, age.models.Vertex):
                    graph.add_node(entity.id, label=entity.label, attributes=entity.properties)
                elif isinstance(entity, age.models.Edge):
                    graph.add_edge(entity.start_id, entity.end_id, id=entity.id,
                                   label=entity.label, attributes=entity.properties)
        elif isinstance(item, age.models.Vertex):
            graph.add_node(item.id, label=item.label, attributes=item.properties)
        elif isinstance(item, age.models.Edge):
            graph.add_edge(item.start_id, item.end_id, id= item.id,
                           label=item.label, attributes=item.properties)
        else:
            pass
    return graph


#   LOAD TO NETWORKX (From AGE graph)
def load_to_networkx_from_graph(graph_name:str) -> nx.DiGraph:
    graph = nx.DiGraph()
    #Add nodes
    cursor = ag.execCypher("MATCH (n) RETURN (n)")
    for row in cursor:
        graph.add_node(row[0].id, label = row[0].label, attributes=row[0].properties)
    #Add edges
    cursor = ag.execCypher("MATCH ()-[e]->() RETURN (e)")
    for row in cursor:
        graph.add_edge(row[0].start_id, row[0].end_id, label = row[0].label,
                       id =row[0].id, attributes=row[0].properties)
    return graph


#   LOAD TO AGE (From NetworkX)
def load_from_networkx(G:nx.DiGraph(), graph_name:str):
    # Convert Python dictionary to JSON-type string
    def dict_to_JSstring(property):
        items = list(f"{key}:\"{value}\"" for key, value in property.items())
        return "{"+",".join(items)+"}"
    # Create nodes
    for node in G.nodes():
        label = G.nodes[node]['label'].strip("'")
        attributes = dict_to_JSstring(G.nodes[node]['attributes'])
        cursor = ag.execCypher(f"CREATE (n:{label} {attributes}) RETURN n")
        for row in cursor:
            print(row)
    # Create edges
    for start, end in G.edges():
        label = G.edges[start, end]['label'].strip("'")
        attributes = dict_to_JSstring(G.edges[start, end]['attributes'])
        cursor = ag.execCypher(f"""MATCH (a), (b) 
        WHERE  id(a) = {start} AND id(b) = {end} 
        CREATE (a)-[e:{label} {attributes}]->(b) 
        RETURN e""")
        for row in cursor:
            print(row)
