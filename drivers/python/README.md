Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.


# AGE AGType parser and driver support for Python
AGType parser and driver support for [Apache AGE](https://age.apache.org/), graph extension for PostgreSQL.

### Features
* Unmarshal AGE result data(AGType) to Vertex, Edge, Path
* Cypher query support for Psycopg2 PostgreSQL driver (enables to use cypher queries directly)

### Prerequisites
* over Python 3.9
* This module runs on [psycopg2](https://www.psycopg.org/) and [antlr4-python3](https://pypi.org/project/antlr4-python3-runtime/)
```
sudo apt-get update
sudo apt-get install python3-dev libpq-dev
git clone https://github.com/apache/age.git
cd age/drivers/python
```

### Install required packages
```
pip install -r requirements.txt
```

### Test
```
python test_age_py.py \
-host "127.0.0.1" \
-db "postgres" \
-u "postgres" \
-pass "agens" \
-port 5432 \
-gn "test_graph"
```

```
python -m unittest -v test_agtypes.py
```

### Build from source
```
python setup.py install
```

### For more information about [Apache AGE](https://age.apache.org/)
* Apache Age : https://age.apache.org/
* GitHub : https://github.com/apache/age
* Document : https://age.apache.org/age-manual/master/index.html

### Check AGE loaded on your PostgreSQL
Connect to your containerized Postgres instance and then run the following commands:
```
# psql 
CREATE EXTENSION age;
LOAD 'age';
SET search_path = ag_catalog, "$user", public;
```

### Usage
* If you are familiar with Psycopg2 driver : Go to [Jupyter Notebook : Basic Sample](samples/apache-age-basic.ipynb) 
* Simpler way to access Apache AGE [AGE Sample](samples/apache-age-note.ipynb) in Samples.
* Agtype converting samples: [Agtype Sample](samples/apache-age-agtypes.ipynb) in Samples.

### License
Apache-2.0 License
