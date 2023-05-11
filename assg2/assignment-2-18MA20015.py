"""
18MA20015, Harshal Dupare
Big Data Processing, Assignment 2
Dated : 27-3-2023
"""
import sys
from random import randrange
import numpy as np


class Disjoint_Set_Union:
    def __init__(self, n):
        self.parent = list(range(n))
        self.cluster_size = [1] * n

    def find(self, u):
        while self.parent[u] != u:
            u = self.parent[u] = self.parent[self.parent[u]]
        return u

    def union(self, u, v):
        u = self.find(u)
        v = self.find(v)
        if u == v:
            return False
        if self.cluster_size[u] < self.cluster_size[v]:
            u, v = v, u
        self.parent[v] = u
        self.cluster_size[u] += self.cluster_size[v]
        return True


def read_graph_edge_list_format(file_path):
    edges = []
    id2node = set()
    id = 0
    with open(file_path, "r") as f:
        for line in f:
            line = line.split(" ")
            u, v = line[0], line[1]
            u = u.replace("\n", "")
            v = v.replace("\n", "")
            edges.append([u, v])
            id2node.add(u)
            id2node.add(v)
    id2node = list(id2node)
    n = len(id2node)
    node2id = dict()
    for i, node in enumerate(id2node):
        node2id[node] = i

    for i in range(len(edges)):
        edges[i][0] = node2id[edges[i][0]]
        edges[i][1] = node2id[edges[i][1]]

    return n, edges, node2id, id2node


def get_mincut(n, edges):
    dsu = Disjoint_Set_Union(n)
    number_of_nodes = n
    remaining_edge_count = len(edges)

    while number_of_nodes > 2 and remaining_edge_count > 0:
        i = randrange(remaining_edge_count)
        u, v = edges[i]
        # Contract the edge (u, v)
        if dsu.union(u, v):
            # if was not already contracted then
            # number of nodes decreases by 1
            number_of_nodes -= 1
        # swap with the last remaining edge
        edges[i], edges[remaining_edge_count - 1] = (
            edges[remaining_edge_count - 1],
            edges[i],
        )
        remaining_edge_count -= 1

    # Count the number of remaining edges to get the mincut value
    min_cut_value = 0
    for i in range(remaining_edge_count):
        u, v = edges[i]
        if dsu.find(u) != dsu.find(v):
            min_cut_value += 1

    c0 = dsu.find(0)
    community_id = [1]
    for u in range(1, n):
        if dsu.find(u) == c0:
            community_id.append(1)
        else:
            community_id.append(2)

    return min_cut_value, community_id


def get_mincut_iterated(n, edges, max_iter=100):
    # Run the mincut algorithm many times to get a good estimate of the mincut
    min_cut = len(edges)
    min_cut_community_ids = None
    for _ in range(max_iter):
        cut, community_ids = get_mincut(n, edges)
        if cut <= min_cut:
            min_cut = cut
            min_cut_community_ids = community_ids
    return min_cut, min_cut_community_ids


if __name__ == "__main__":
    file_path = sys.argv[1]
    n, edges, node2id, id2node = read_graph_edge_list_format(file_path)

    # estimate of iterations for 1/n wrong probability, but max number of operations allowed is 150000000/C (C is constant)
    # the maximum number of operations allowed was decided so that the number of iterations is limited and the algorithm runs
    # in a reasonable amount of time note that the complexity of the algorithms implemented is O(n + m*logstar(n))
    max_iter = min(
        int((n * (n - 1) / 2 )* (np.log(n) + 1)), int(150000000 / (n + len(edges)))
    )

    min_cut, community_id = get_mincut_iterated(n, edges, max_iter)
    print(f"Estimate of Mincut value via Karger algorithm: {min_cut}")
    for u in range(len(community_id)):
        print(f"{id2node[u]} {community_id[u]}")