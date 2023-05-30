# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import age
from age.models import *
import unittest
import decimal
from age_networkx import *
import networkx as nx

DSN = "host=172.17.0.2 port=5432 dbname=postgres user=postgres password=agens"
TEST_GRAPH_NAME = "test"

class TestSupportNetworkx(unittest.TestCase):
    ag = None
    def setUp(self):
        print("\n")
        print("Connecting to Test Graph.....")
        # Setting up connection to work with Graph
        self.ag = age.Age().connect(TEST_GRAPH_NAME, dsn=DSN)

    def tearDown(self):
        # Clear test data
        print("Deleting Test Graph.....")
        age.deleteGraph(self.ag.connection, self.ag.graphName)
        self.ag.close()
        

    def testageToNetworkx(self):
        print("[Test AGE to NetworkX]")
        ag = self.ag
        # Create nodes
        ag.execCypher("CREATE (n:Person {name: %s}) ", params=('Jack',))
        ag.execCypher("CREATE (n:Person {name: %s}) ", params=('Andy',))
        ag.execCypher("CREATE (n:Person {name: %s}) ", params=('Smith',))
        ag.commit()
        # Create Edges
        ag.execCypher("""MATCH (a:Person), (b:Person)
                      WHERE a.name = 'Andy' AND b.name = 'Jack'
                      CREATE (a)-[r:workWith {weight: 3}]->(b)""")
        ag.execCypher("""MATCH (a:Person), (b:Person) 
                    WHERE  a.name = %s AND b.name = %s 
                    CREATE p=((a)-[r:workWith {weight: 10}]->(b)) """, params=('Jack', 'Smith',))
        ag.commit()

        G = ageToNetworkx(self.ag.connection, TEST_GRAPH_NAME)
        
        # Check that the G has the expected properties
        self.assertIsInstance(G, nx.DiGraph)
        # Check that the G has the correct number of nodes and edges
        self.assertEqual(len(G.nodes), 3)
        self.assertEqual(len(G.edges), 2)
        # Check that the node properties are correct
        for node in G.nodes:
            self.assertEqual(int, type(node))
            self.assertEqual(G.nodes[node]['label'], 'Person')
            self.assertIn('name', G.nodes[node]['properties'])
            self.assertIn('properties', G.nodes[node])
            self.assertEqual(str, type(G.nodes[node]['label']))

        # Check that the edge properties are correct
        for edge in G.edges:
            self.assertEqual(tuple, type(edge))
            self.assertEqual(int, type(edge[0]) and type(edge[1]))
            self.assertEqual(G.edges[edge]['label'], 'workWith')
            self.assertIn('weight', G.edges[edge]['properties'])
            self.assertEqual(int, type(G.edges[edge]['properties']['weight']))
            

    def testnetworkxToAge(self):
        print("[Test NetworkX to AGE]")
        ag = self.ag
        # Create NetworkX graph
        G = nx.DiGraph()
        G.add_node(1, label='Person', properties={'name': 'Tito', 'age': '26', 'id': 1})
        G.add_node(2, label='Person', properties={'name': 'Austen', 'age': '26', 'id': 2})
        G.add_edge(1, 2, label='KNOWS', properties={'since': '1997', 'start_id': 1, 'end_id': 2})

        networkxToAge(self.ag.connection, G, TEST_GRAPH_NAME)

        # Check that node(s) were created
        cursor = ag.execCypher('MATCH (n) RETURN n')
        result = cursor.fetchall()
        # Check number of vertices created
        self.assertEqual(len(result), 2)
        # Checks if type of property in query output is a Vertex
        self.assertEqual(Vertex, type(result[0][0]))
        self.assertEqual(Vertex, type(result[1][0]))

        # Check that edge(s) was created
        cursor = ag.execCypher('MATCH ()-[e]->() RETURN e')
        result = cursor.fetchall()
        # Check number of edge(s) created
        self.assertEqual(len(result), 1)
        # Checks if type of property in query output is an Edge
        self.assertEqual(Edge, type(result[0][0]))
    
if __name__ == '__main__':
    unittest.main()
