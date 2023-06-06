import age, csv, json
from typing import Tuple

def get_contents_from_csv(file_path) -> Tuple[list, list]:
    file = open(file_path)
    csvreader = csv.reader(file)
    header = []
    header = next(csvreader)
    records = []
    for record in csvreader:
        records.append(record)
    return header, records

def make_map_csv(header: str) -> str:
    properties = ''
    for each_property in header:
        properties += each_property + ': %s, '
    return properties[0: len(properties)-2]


def make_map_json(d, prop, record) -> None:
    for k, v in d.items():
        if isinstance(v, dict):
            (prop.append(',' + k + ': {') 
                if prop[-1][-1] == '}' 
                else prop.append(k + ': {'))
            make_map_json(v, prop, record)
        else:
            record.append(str(v))
            (prop.append(',' + k + ': %s,') 
                if prop[-1][-1] == '}' 
                else prop.append(k + ': %s,'))
    if prop[-1].endswith(','):
        prop[-1] = prop[-1][:-1]
    prop.append('}')

def load_labels_from_file(ag: age.age.Age, 
                          file_path: str, label_name: str) -> None:
    def load_labels_from_csv() -> None:
        header, records = get_contents_from_csv(file_path)
        properties = make_map_csv(header)
        for record in records:
            if label_name == '':
                cursor = ag.execCypher('CREATE (n' + '{' + 
                                       properties + '}) RETURN n',
                                       params=tuple(record))
            else:
                cursor = ag.execCypher('CREATE (n: ' + label_name + '{' + 
                                       properties + '}) RETURN n', 
                                       params=tuple(record))
        ag.commit()
        
    def load_labels_from_json() -> None:
        with open(file_path, 'r') as openfile:
            json_object = json.load(openfile)
        for i in json_object:
            record, prop = [], ['{']
            make_map_json(i, prop, record)
            prop = ''.join(prop)
            if label_name == '':
                cursor = ag.execCypher('CREATE (n' + prop + ') RETURN n', 
                                       params=tuple(record))
            else:
                cursor = ag.execCypher('CREATE (n: ' + label_name + prop + 
                                       ') RETURN n', 
                                       params=tuple(record))
        ag.commit()
        
        
    if file_path.endswith('csv'):
        load_labels_from_csv()
    elif file_path.endswith('json'):
        load_labels_from_json()
    

def load_edges_from_file(ag: age.age.Age, 
                         file_path: str, label_name: str) -> None:

    def load_edges_from_csv()-> None:
        def get_property_names() -> Tuple[list, list, list]:
            v1_prop_names, v2_prop_names, e_prop_names = [], [], []
            v1_index = header.index('start_vertex_type')
            v2_index = header.index('end_vertex_type')
            for i, property_name in enumerate(header):
                if i < v1_index:
                    v1_prop_names.append(property_name)
                elif (i == v1_index or i == v2_index):
                    continue
                elif (i < v2_index):
                    v2_prop_names.append(property_name)
                else:
                    e_prop_names.append(property_name)
            return v1_prop_names, v2_prop_names, e_prop_names
                
            
        def get_property_values() -> Tuple[list, list, list]:
            v1_prop_values, v2_prop_values, e_prop_values = [], [], []
            for i in range(0, num_v1):
                v1_prop_values.append(record[i])
            for i in range(num_v1+1, num_v1+1+num_v2):
                v2_prop_values.append(record[i])
            for i in range(num_v2+1, num_v2+1+num_edge):
                e_prop_values.append(record[i])
            return v1_prop_values, v2_prop_values, e_prop_values


        header, records = get_contents_from_csv(file_path)
        
        v1_prop_names, v2_prop_names, e_prop_names = get_property_names()
        v1_map, v2_map, e_map = (make_map_csv(v1_prop_names), 
                                make_map_csv(v2_prop_names), 
                                make_map_csv(e_prop_names))
        num_v1, num_v2, num_edge = (len(v1_prop_names), 
                                    len(v2_prop_names), 
                                    len(e_prop_names))
        for record in records:
            (v1_prop_values, 
            v2_prop_values, 
            e_prop_values) = get_property_values()
            v1_label, v2_label = record[num_v1], record[num_v1+num_v2+1]
            cursor = ag.execCypher('MATCH (u: ' + v1_label + '{' + v1_map + 
                                   '}), (v: ' + v2_label  + '{' + v2_map +
                                   '}) CREATE (u)-[e: ' + label_name + 
                                   '{' + e_map + '}' + ']->(v) RETURN e', 
                                params=tuple(v1_prop_values)
                                +tuple(v2_prop_values)
                                +tuple(e_prop_values)) 
        ag.commit()
        
    def load_edges_from_json()-> None:
        with open(file_path, 'r') as openfile:
            json_object = json.load(openfile)
        for i in json_object:
            
            v1_prop_vals, v1_prop = [], ['{']
            make_map_json(i['start_vertex'], v1_prop, v1_prop_vals)
            v1_prop = ''.join(v1_prop)
            v1_label = i['start_vertex_type']
            v2_prop_vals, v2_prop = [], ['{']
            make_map_json(i['end_vertex'], v2_prop, v2_prop_vals)
            v2_prop = ''.join(v2_prop)
            v2_label = i['end_vertex_type']
            e_prop_vals, e_prop = [], ['{']
            make_map_json(i['edge'], e_prop, e_prop_vals)
            e_prop = ''.join(e_prop)
            cursor = ag.execCypher('MATCH (u: ' + v1_label + v1_prop + 
                                   '), (v: ' + v2_label + v2_prop + 
                                   ') CREATE (u)-[e: ' + label_name + e_prop + 
                                   ']->(v) RETURN e', 
                                   params = tuple(v1_prop_vals) 
                                   + tuple(v2_prop_vals)
                                   + tuple(e_prop_vals))
        ag.commit()
            
    if file_path.endswith('csv'):
        load_edges_from_csv()
    elif file_path.endswith('json'):
        load_edges_from_json()
        
        
def load_labels_into_file(ag: age.age.Age) -> None:
    
    def load_labels_into_csv():
        file_path = './age_load/data/' + row[0] + '.csv'
        with open(file_path, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(list((f_vertices[0][0].properties).keys()))
            for each_vertex in f_vertices:
                writer.writerow(list((each_vertex[0].properties).values()))
        file.close()
        
    def load_labels_into_json():
        file_path = './age_load/data/' + row[0] + '.json'
        json_list = [vertex[0].properties for vertex in f_vertices]
        json_object = json.dumps(json_list, indent=4)
        with open(file_path, mode='w') as file:
            file.write(json_object)
        file.close()
        
    # TODO -- this is surely an overhead
    cursor = ag.execCypher('MATCH(u) RETURN DISTINCT labels(u)[0]')
    for row in cursor:
        vertices = ag.execCypher('MATCH (u:' + row[0] + ') RETURN u')
        f_vertices = vertices.fetchall()
        var = any(isinstance(i,dict) for j in f_vertices 
                  for i in j[0].properties.values())
        load_labels_into_json() if var else load_labels_into_csv()
    
def load_edges_into_file(ag: age.age.Age) -> None:
    
    cursor = ag.execCypher('MATCH (u)-[e]->(v) RETURN DISTINCT type(e)')
    
    def load_edges_into_csv():
        file_path = './age_load/data/' + row[0] + '.csv'
        with open(file_path, mode='w', newline='') as file:
            writer = csv.writer(file)
            header = (list(f_edges[0][0][0].properties.keys()) +
                      ['start_vertex_type'] +
                      list(f_edges[0][0][2].properties.keys()) +
                      ['end_vertex_type'] +
                      list(f_edges[0][0][1].properties.keys()))
            writer.writerow(header)
            for each_edge in f_edges:
                writer = csv.writer(file)
                record =  (list(each_edge[0][0].properties.values()) + 
                           [each_edge[0][0].label, ] + 
                           list(each_edge[0][2].properties.values()) + 
                           [each_edge[0][2].label, ] + 
                           list(each_edge[0][1].properties.values()))
                writer.writerow(record)
        file.close()
    def load_edges_into_json():
        file_path = './age_load/data/' + row[0] + '.json'
        json_list = []
        for path in f_edges:
            dict1 = {}
            dict1['start_vertex'] = path[0][0].properties
            dict1['start_vertex_type'] = path[0][0].label
            dict1['end_vertex'] = path[0][2].properties
            dict1['end_vertex_type'] = path[0][2].label
            dict1['edge'] = path[0][1].properties
            # dict1.update(path[0][1].properties)
            json_list.append(dict1)
        json_object = json.dumps(json_list, indent=4)
        with open(file_path, mode='w') as file:
            file.write(json_object)
        file.close()
            
    for row in cursor:
        edges = ag.execCypher('MATCH p = (u)-[e:' + row[0] + 
                              ']->(v) RETURN p')
        f_edges = edges.fetchall()
        var = (any(isinstance(i,dict) for j in f_edges 
                  for i in j[0][1].properties.values()) or 
               any(isinstance(i,dict) for j in f_edges 
                  for i in j[0][0].properties.values()) or 
               any(isinstance(i,dict) for j in f_edges 
                  for i in j[0][2].properties.values()))
        load_edges_into_json() if var else load_edges_into_csv()
        
