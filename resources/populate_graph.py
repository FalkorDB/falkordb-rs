#! /usr/bin/env python

from falkordb import FalkorDB, Node, Edge
import os
import csv

db = FalkorDB()

graph = db.select_graph("imdb")

if graph.name in db.list_graphs():
    exit(0)

movies = {}
node_count = 0

with open(os.path.dirname(os.path.abspath(__file__)) + '/movies.csv', 'r') as f:
    reader = csv.reader(f, delimiter=',')
    for row in reader:
        title = row[0]
        genre = row[1]
        votes = int(row[2])
        rating = float(row[3])
        year = int(row[4])

        node = Node(alias=f"n_{node_count}", labels="movie", properties={'title': title,
                                                                         'genre': genre,
                                                                         'votes': votes,
                                                                         'rating': rating,
                                                                         'year': year})
        movies[title] = node
        node_count += 1

# Load actors entities
actors = {}

edges = []
with open(os.path.dirname(os.path.abspath(__file__)) + '/actors.csv', 'r') as f:
    reader = csv.reader(f, delimiter=',')
    for row in reader:
        name = row[0]
        yearOfBirth = int(row[1])
        movie = row[2]
        # All age calculations are done where 2019 is the the current year.
        age = 2019 - yearOfBirth

        if name not in actors:
            node = Node(alias=f"n_{node_count}", labels="actor", properties={'name': name, 'age': age})
            actors[name] = node
            node_count += 1

        if movie in movies:
            edges.append(Edge(actors[name], "act", movies[movie]))

edges_str = [str(e) for e in edges]
nodes_str = [str(n) for n in actors.values()] + [str(n) for n in movies.values()]
graph.query(f"CREATE {','.join(nodes_str + edges_str)}")

graph.create_node_fulltext_index("actor", "name")
graph.create_node_range_index("actor", "age")
