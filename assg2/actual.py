import networkx as nx
from networkx.algorithms.flow import minimum_cut
import sys

g = nx.read_edgelist(sys.argv[1])

isconnected = nx.is_connected(g)

if isconnected:
    min_cut = len(nx.minimum_edge_cut(g))
    print("Min cut:", min_cut )
else:
    print("Min cut: 0")

print(50*"#")