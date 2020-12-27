#!/usr/bin/env python
"""mapper.py"""


import sys
import string

# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    # split the line into words
    line = line.strip('\n')
    line = line.split(',')
    if line[6] != '0' and line[1] != 'item_id' and line[5] == '1111':
        print(line[1], '\t', 1)


