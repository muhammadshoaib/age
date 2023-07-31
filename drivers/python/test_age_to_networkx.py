import age
from age.models import *
import unittest
import networkx as nx
from age.networkx import *

DSN = "host=172.17.0.2 port=5432 dbname=postgres user=postgres password=agens"
TEST_GRAPH_NAME = "test_graph"

class TestAgeToNetworkx(unittest.TestCase):
    ag = None 
    def setUp(self):        
        self.ag = age.Age().connect(TEST_GRAPH_NAME, dsn=DSN)

    def tearDown(self):
        age.deleteGraph(self.ag.connection, self.ag.graphName)
        self.ag.close()

    def compare_networkX(self, G, H):
        if G.number_of_nodes() != H.number_of_nodes():
            return False
        if G.number_of_edges() != H.number_of_edges():
            return False
        # test nodes
        nodes_G, nodes_H = G.number_of_nodes(), H.number_of_nodes()
        markG, markH = [0]*nodes_G, [0]*nodes_H
        nodes_list_G, nodes_list_H = list(G.nodes), list(H.nodes)
        print(nodes_list_G, nodes_list_H)
        for i in range(0, nodes_G):
            for j in range(0, nodes_H):
                if markG[i]==0 and markH[j]==0:
                    node_id_G = nodes_list_G[i]
                    property_G = G.nodes[node_id_G]

                    node_id_H = nodes_list_H[j]
                    property_H = H.nodes[node_id_H]
                    print("prp  : ", property_G, property_H)
                    if property_G == property_H:
                        markG[i] = 1
                        markH[j] = 1

        if any(elem == 0 for elem in markG):
            return False
        if any(elem == 0 for elem in markH):
            return False
        
        # test edges
        edges_G, edges_H = G.number_of_edges(), H.number_of_edges()
        markG, markH = [0]*edges_G, [0]*edges_H
        edges_list_G, edges_list_H = list(G.edges), list(H.edges)

        for i in range(0, edges_G):
            for j in range(0, edges_H):
                if markG[i]==0 and markH[j]==0:
                    source_G, target_G = edges_list_G[i]
                    property_G = G.edges[source_G, target_G]

                    source_H, target_H = edges_list_H[j]
                    property_H = H.edges[source_H, target_H]

                    if property_G == property_H:
                        markG[i] = 1
                        markH[j] = 1

        if any(elem == 0 for elem in markG):
            print("3")
            return False
        if any(elem == 0 for elem in markH):
            print("4")
            return False

        return True
    
    def testAgeToNetowrkX1(self):
        # Expected Graph
        # Empty Graph
        G = nx.DiGraph()
        

        # AGE Graph


        # Convert Apache AGE to NetworkX 
        H = age_to_networkx(self.ag.connection, TEST_GRAPH_NAME)

        self.assertTrue(self.compare_networkX(G, H))

    def testAgeToNetowrkX2(self):
        # Expected Graph
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
        

        # AGE Graph
        self.ag.execCypher("CREATE (:l1 {name: 'n1', weight: '5'})")
        self.ag.execCypher("CREATE (:l1 {name: 'n2', weight: '4'})")
        self.ag.execCypher("CREATE (:l1 {name: 'n3', weight: '9'})")
        
        self.ag.execCypher("""MATCH (a:l1), (b:l1)
                            WHERE a.name = 'n1' AND b.name = 'n2'
                            CREATE (a)-[e:e1 {property:'graph'}]->(b)""")
        self.ag.execCypher("""MATCH (a:l1), (b:l1)
                            WHERE a.name = 'n2' AND b.name = 'n3'
                            CREATE (a)-[e:e2 {property:'node'}]->(b)""")

        # Convert Apache AGE to NetworkX 
        H = age_to_networkx(self.ag.connection, TEST_GRAPH_NAME)


        self.assertTrue(self.compare_networkX(G, H))

    def testAgeToNetowrkX3(self):
        # Expected Graph
        G = nx.DiGraph()

        G.add_node('1', 
            label='l1',
            properties={'name' : 'n1'})
        G.add_node('2', 
            label='l1', 
            properties={'name': 'n2'})
        G.add_node('3', 
            label='l1', 
            properties={'name': 'n3'})
        

        # AGE Graph
        self.ag.execCypher("CREATE (:l1 {name: 'n1'})")
        self.ag.execCypher("CREATE (:l1 {name: 'n2'})")
        self.ag.execCypher("CREATE (:l1 {name: 'n3'})")
        

        # Convert Apache AGE to NetworkX 
        H = age_to_networkx(self.ag.connection, TEST_GRAPH_NAME)

        self.assertTrue(self.compare_networkX(G, H))

if __name__=="__main__":
    unittest.main()