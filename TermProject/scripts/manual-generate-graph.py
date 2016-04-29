#!/usr/bin/python
# -*- coding: utf-8 -*-

from sys import argv
from random import randint

if __name__ == "__main__":
    n = int(argv[1])
    f = open(argv[2], 'w')
    prob = int(argv[3]) if len(argv)> 3 else 50
    for n_from in xrange(0, n):
        for n_to in xrange(n_from, n):
            if randint(0, 100) <= prob:
                f.write(' '.join(map(str, [n_from, n_to])) + '\n')
