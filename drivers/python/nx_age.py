import age
import networkx as nx
import matplotlib.pyplot as plt
from psycopg2 import extensions as ext

# Loads AGE graph or query output to NetworkX
def load_to_networkx(graph_name:str, DSN:str, query_output:ext.cursor=None) -> nx.DiGraph:
    def addNode(node):
        graph.add_node(node.id, label=node.label, properties=node.properties)
    def addEdge(edge):
        graph.add_edge(edge.start_id, edge.end_id, id= edge.id,
                           label=edge.label, properties=edge.properties)   
    def addPath(path):
        for entity in path:
            if isinstance(entity, age.models.Path):
                addPath(entity)
            elif isinstance(entity, age.models.Vertex):
                addNode(entity)
            elif isinstance(entity, age.models.Edge):
                addEdge(entity)
    
    ag = age.connect(graph=graph_name, dsn=DSN)
    graph = nx.DiGraph()

    if (query_output != None):
        try:
            query_output = query_output.fetchall()
            for row in query_output:
                if isinstance(row[0], age.models.Path):
                    addPath(row[0])
                elif isinstance(row[0], age.models.Vertex):
                    addNode(row[0])
                elif isinstance(row[0], age.models.Edge):
                    addEdge(row[0])
        except Exception as e:
            print(type(e), e)
    else:
        try:
            # Add Nodes
            cursor = ag.execCypher("MATCH (n) RETURN (n)")
            for row in cursor:
                addNode(row[0])
            # Add edges
            cursor = ag.execCypher("MATCH ()-[e]->() RETURN (e)")
            for row in cursor:
                addEdge(row[0])
        except Exception as e:
            print(type(e), e)   
    return graph