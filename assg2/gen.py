import networkx as nx
import os
import time
import sys
# generate a random graph with 10 nodes and probability of edge creation of 0.3
G = nx.gnp_random_graph(int(sys.argv[1]), float(sys.argv[2]))

# save the edge list of the graph to a text file named "input"
nx.write_edgelist(G, os.path.join("tests",f"{int(sys.argv[1])}_{float(sys.argv[2]):0.3}_{int(time.time())%1000000}.txt"), data=False)