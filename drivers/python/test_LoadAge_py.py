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

import unittest
import os
import age
from age.models import *
from age.LoadStoreAge import LoadAge
from tempfile import NamedTemporaryFile
import argparse

DSN = "host=127.0.0.1 port=5432 dbname=postgres user=postgres password=agens"

class TestAgeLoad(unittest.TestCase):
    ag = None
    def setUp(self):
        print("")
        print("Connecting to Test Graph.....")
        # Setting up connection to work with Graph
        TEST_DB = self.args.database
        TEST_USER = self.args.user
        TEST_PASSWORD = self.args.password
        TEST_PORT = self.args.port
        TEST_HOST = self.args.host
        TEST_GRAPH_NAME = self.args.graphName
        self.ag = age.connect(graph=TEST_GRAPH_NAME, host=TEST_HOST, port=TEST_PORT, dbname=TEST_DB, user=TEST_USER, password=TEST_PASSWORD)

    def tearDown(self):
        # Clear test data
        print("Deleting Test Graph.....")
        age.deleteGraph(self.ag.connection, self.ag.graphName)
        self.ag.close()
    
    def testload_labels_from_csv(self):
        print("\n----------------------------------")
        print("Test 1: Testing load_labels_from_csv")
        print("----------------------------------\n")

        ag = self.ag

        # Create temporary CSV file with test data
        file_data = 'id,name,age\n1,John,30\n2,Jane,25\n'
        with NamedTemporaryFile(suffix=".csv", delete=False) as temp_file:
            temp_file.write(file_data.encode("utf-8"))
            temp_file_path = temp_file.name

        load_labels_from_file(ag.connection, self.ag.graphName, temp_file_path, "Person")

        cursor = ag.execCypher('MATCH (n:Person) RETURN n')
        result = cursor.fetchall()

        # Assertions to check if data is loaded correctly
        self.assertEqual(len(result), 2)
        self.assertEqual(Vertex, type(result[0][0]))
        self.assertEqual(Vertex, type(result[1][0]))
        self.assertEqual(result[0][0]["name"], "John")
        self.assertEqual(result[0][0]["age"], str(30))
        self.assertEqual(result[1][0]["name"], "Jane")
        self.assertEqual(result[1][0]["age"], str(25))

        # Clean up temporary file
        os.remove(temp_file_path)
        print("Test 1: Successful...\n")
    
    def testload_labels_from_json(self):
        print("\n-----------------------------------")
        print("Test 2: Testing load_labels_from_json")
        print("-----------------------------------\n")

        ag = self.ag

        # Create temporary JSON file with test data
        file_data = '[{"name": "John", "age": 30}, {"name": "Jane", "age": 25}]'
        with NamedTemporaryFile(suffix=".json", delete=False) as temp_file:
            temp_file.write(file_data.encode("utf-8"))
            temp_file_path = temp_file.name

        load_labels_from_file(ag.connection, self.ag.graphName, temp_file_path, "Person")

        cursor = ag.execCypher("MATCH (n:Person) RETURN n")
        result = cursor.fetchall()
        
        # Assertions to check if data is loaded correctly
        self.assertEqual(len(result), 2)
        self.assertEqual(Vertex, type(result[0][0]))
        self.assertEqual(Vertex, type(result[1][0]))
        self.assertEqual(result[0][0]["name"], "John")
        self.assertEqual(result[0][0]["age"], 30)
        self.assertEqual(result[1][0]["name"], "Jane")
        self.assertEqual(result[1][0]["age"], 25)

        # Clean up temporary file
        os.remove(temp_file_path)
        print("Test 2: Successful...\n")

    def testload_edges_from_csv(self):
        print("\n---------------------------------")
        print("Test 3: Testing load_edges_from_csv")
        print("---------------------------------\n")

        ag = self.ag

        # Create nodes
        ag.execCypher("CREATE (n:Person {id: 1, name: %s, age: 30}) ", params=('John',))
        ag.execCypher("CREATE (n:Friend {id: 2, name: %s, age: 25}) ", params=('Jane',))
        ag.commit()

        # Create temporary CSV file with test data
        file_data = "start_id,start_vertex_type,end_id,end_vertex_type,weight\n1,Person,2,Friend,3\n"
        with NamedTemporaryFile(suffix=".csv", delete=False) as temp_file:
            temp_file.write(file_data.encode("utf-8"))
            temp_file_path = temp_file.name

        load_edges_from_file(ag.connection, self.ag.graphName, temp_file_path, "KNOWS")

        cursor = ag.execCypher("MATCH ()-[e]->() RETURN e")
        result = cursor.fetchall()

        # Assertions to check if data is loaded correctly
        self.assertEqual(len(result), 1)
        self.assertEqual(Edge, type(result[0][0]))
        self.assertEqual(result[0][0]["weight"], str(3))
        
        # Clean up temporary file
        os.remove(temp_file_path)
        print("Test 3: Successful...\n")
    
    def testload_edges_from_json(self):
        print("\n----------------------------------")
        print("Test 4: Testing load_edges_from_json")
        print("----------------------------------\n")
        ag = self.ag

        # Create nodes
        ag.execCypher("CREATE (n:Person {id: 1, name: %s, age: 30}) ", params=('John',))
        ag.execCypher("CREATE (n:Friend {id: 2, name: %s, age: 25}) ", params=('Jane',))
        ag.commit()

        # Create temporary JSON file with test data
        file_data = '''
        [
            {
                "start_id": 1,
                "start_vertex_type": "Person",
                "end_id": 2,
                "end_vertex_type": "Friend",
                "properties": {"weight": 3}
            }
        ]
        '''

        with NamedTemporaryFile(suffix=".json", delete=False) as temp_file:
            temp_file.write(file_data.encode("utf-8"))
            temp_file_path = temp_file.name

        load_edges_from_file(ag.connection, self.ag.graphName, temp_file_path, "KNOWS")

        cursor = ag.execCypher("MATCH ()-[e]->() RETURN e")
        result = cursor.fetchall()

        # Assertions to check if data is loaded correctly
        self.assertEqual(len(result), 1)
        self.assertEqual(Edge, type(result[0][0]))
        self.assertEqual(result[0][0]["weight"], 3)

        # Clean up temporary file
        os.remove(temp_file_path)
        print("Test 4: Successful...\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    
    parser.add_argument('-host', 
                        '--host', 
                        help='Optional Host Name. Default Host is "127.0.0.1" ', 
                        default="127.0.0.1")
    parser.add_argument('-port', 
                        '--port', 
                        help='Optional Port Number. Default port no is 5432', 
                        default=5432)
    parser.add_argument('-db', 
                        '--database', 
                        help='Required Database Name', 
                        required=True)
    parser.add_argument('-u', 
                        '--user', 
                        help='Required Username Name', 
                        required=True)
    parser.add_argument('-pass', 
                        '--password', 
                        help='Required Password for authentication', 
                        required=True)
    parser.add_argument('-gn', 
                        '--graphName', 
                        help='Optional Graph Name to be created. Default graphName is "test_graph"', 
                        default="test_graph")

    args = parser.parse_args()
    suite = unittest.TestSuite()
    suite.addTest(TestAgeLoad('testload_labels_from_csv'))
    suite.addTest(TestAgeLoad('testload_labels_from_json'))
    suite.addTest(TestAgeLoad('testload_edges_from_csv'))
    suite.addTest(TestAgeLoad('testload_edges_from_json'))
    TestAgeLoad.args = args
    unittest.TextTestRunner().run(suite)