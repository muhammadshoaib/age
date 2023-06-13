import age
from age.models import *
import unittest
import decimal
import networkx as nx
from age_networkx import *

DSN = "host=172.17.0.2 port=5432 dbname=postgres user=postgres password=agens"
ORIGINAL_GRAPH = "original_graph"
EXPECTED_GRAPH = "expected_graph"

class TestNetworkxToAGE(unittest.TestCase):
    ag1 = None
    ag2 = None 
    def setUp(self):
        self.ag1 = age.Age().connect(ORIGINAL_GRAPH, dsn=DSN)
        self.ag2 = age.Age().connect(EXPECTED_GRAPH, dsn=DSN)


    def tearDown(self):
        age.deleteGraph(self.ag1.connection, self.ag1.graphName)
        age.deleteGraph(self.ag2.connection, self.ag2.graphName)
        self.ag1.close()
        self.ag2.close()
    
    def compare_age(self, age1, age2):
        cursor = age1.execCypher("MATCH (v) RETURN v")
        g_nodes = cursor.fetchall()

        cursor = age1.execCypher("MATCH ()-[r]->() RETURN r")
        g_edges = cursor.fetchall()

        cursor = age2.execCypher("MATCH (v) RETURN v")
        h_nodes = cursor.fetchall()

        cursor = age2.execCypher("MATCH ()-[r]->() RETURN r")
        h_edges = cursor.fetchall()

        
        
        if len(g_nodes)!=len(h_nodes) or len(g_edges)!=len(h_edges):
            return False
        
        # test nodes
        nodes_G, nodes_H = len(g_nodes), len(h_nodes)
        markG, markH = [0]*nodes_G, [0]*nodes_H

        # print(g_nodes[0][0].properties)
        # return True
        for i in range(0, nodes_G):
            for j in range(0, nodes_H):
                if markG[i]==0 and markH[j]==0:
                    property_G = g_nodes[i][0].properties
                    property_G['label'] = g_nodes[i][0].label
                    property_G.pop('id')

                    property_H = h_nodes[i][0].properties
                    property_H['label'] = h_nodes[i][0].label

                    if property_G == property_H:
                        markG[i] = 1
                        markH[j] = 1

        if any(elem == 0 for elem in markG):
            return False
        if any(elem == 0 for elem in markH):
            return False
        
        # test edges
        edges_G, edges_H = len(g_edges), len(h_edges)
        markG, markH = [0]*edges_G, [0]*edges_H


        for i in range(0, edges_G):
            for j in range(0, edges_H):
                if markG[i]==0 and markH[j]==0:
                    property_G = g_edges[i][0].properties
                    property_G['label'] = g_edges[i][0].label
                    property_G.pop('start_id')
                    property_G.pop('end_id')

                    property_H = h_edges[i][0].properties
                    property_H['label'] = h_edges[i][0].label

                    if property_G == property_H:
                        markG[i] = 1
                        markH[j] = 1

        if any(elem == 0 for elem in markG):
            return False
        if any(elem == 0 for elem in markH):
            return False
        
        return True

    def testAgeToNetowrkX1(self):
        # Expected Graph
        self.ag2.execCypher("CREATE (:l1 {name: 'n1', weight: '5'})")
        self.ag2.execCypher("CREATE (:l1 {name: 'n2', weight: '4'})")
        self.ag2.execCypher("CREATE (:l1 {name: 'n3', weight: '9'})")
        
        self.ag2.execCypher("""MATCH (a:l1), (b:l1)
                            WHERE a.name = 'n1' AND b.name = 'n2'
                            CREATE (a)-[e:e1 {property:'graph'}]->(b)""")
        self.ag2.execCypher("""MATCH (a:l1), (b:l1)
                            WHERE a.name = 'n2' AND b.name = 'n3'
                            CREATE (a)-[e:e2 {property:'node'}]->(b)""")
        
        # NetworkX Graph
        G = nx.DiGraph()

        G.add_node('1', 
            label='l1',
            properties={'name' : 'n1',
                        'weight' : '5'})
        G.add_node('2', 
            label='l1', 
            properties={'name': 'n2' ,
                        'weight' : '4'})
        G.add_node('3', 
            label='l1', 
            properties={'name': 'n3' ,
                        'weight' : '9'})
        G.add_edge('1', '2', label='e1', properties={'property' : 'graph'} )
        G.add_edge('2', '3', label='e2', properties={'property' : 'node'} )

        # Convert Apache AGE to NetworkX 
        networkxToAge(self.ag1.connection, G, ORIGINAL_GRAPH)

        self.assertTrue(self.compare_age(self.ag1, self.ag2))

    def testAgeToNetowrkX2(self):
        # Expected Graph
        # Empty Graph
        
        # NetworkX Graph
        G = nx.DiGraph()

        

        # Convert Apache AGE to NetworkX 
        networkxToAge(self.ag1.connection, G, ORIGINAL_GRAPH)

        self.assertTrue(self.compare_age(self.ag1, self.ag2))

    def testAgeToNetowrkX3(self):
        # Expected Graph
        self.ag2.execCypher("CREATE (:l1 {name: 'n1'})")
        self.ag2.execCypher("CREATE (:l1 {name: 'n2'})")
        self.ag2.execCypher("CREATE (:l1 {name: 'n3'})")
                
        # NetworkX Graph
        G = nx.DiGraph()

        G.add_node('1', 
            label='l1',
            properties={'name': 'n1'})
        G.add_node('2', 
            label='l1', 
            properties={'name': 'n2' })
        G.add_node('3', 
            label='l1', 
            properties={'name': 'n3'})

        # Convert Apache AGE to NetworkX 
        networkxToAge(self.ag1.connection, G, ORIGINAL_GRAPH)

        self.assertTrue(self.compare_age(self.ag1, self.ag2))

if __name__=="__main__":
    unittest.main()