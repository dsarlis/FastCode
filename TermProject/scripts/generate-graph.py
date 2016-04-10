#!/usr/bin/python
# -*- coding: utf-8 -*-

#Install pip install networkx

from networkx.generators import dense_gnm_random_graph
from networkx.readwrite import write_edgelist
from sys import argv

if __name__ == "__main__":
    g = dense_gnm_random_graph(int(argv[1]), int(argv[2]))
    write_edgelist(g, argv[3], data=False)
