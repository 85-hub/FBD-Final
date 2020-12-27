#!/usr/bin/env python
"""mapper.py"""


import sys
import string

# input comes from STDIN (standard input)
lis = []
for line in sys.stdin:
    # remove leading and trailing whitespace
    # split the line into words
    line = line.strip('\n')
    line = line.split(',')
    print(line[0])
    if len(line) == 3:
        if line[1] != 'age_range':
            print('heare')
            if line[1] == '1' or line[1] == '2' or line[1] == '3':
                # print(line[0], '\t', 'file1')
                print('ok')
                lis.append(line[0])
                print(lis)
    if len(line) == 7:
        if line[6] != '0' and line[1] != 'item_id' and line[5] == '1111' and line[0] in lis:
            print(line[3], '\t', 1)




