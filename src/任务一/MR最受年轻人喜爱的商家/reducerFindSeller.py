#!/usr/bin/env python
"""reducer.py"""

from operator import itemgetter
import sys

current_word = None
current_count = 0
word = None
dic = {}

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace

    line = line.strip( )

    # parse the input we got from mapper.py
    word, count = line.split('\t', 1)
    word = word.strip( )

    # convert count (currently a string) to int
    try:
        count = int(count)
    except ValueError:
        # count was not a number, so silently
        # ignore/discard this line
        continue

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: word) before it is passed to the reducer
    if current_word == word:
        current_count += count
    else:
        if current_word:
            # write result to dict
            idc = {current_word: current_count}
            dic.update(idc)
        #            print ('%s\t%s' % (current_word, current_count))
        #            print(idc)
        current_count = count
        current_word = word

# do not forget to output the last word if needed!
if current_word == word:
    #    print ('%s\t%s' % (current_word, current_count))
    idc = {current_word: current_count}
    dic.update(idc)

# sort dict according to its reversely
c = sorted(dic.items(), key=lambda x: x[1], reverse=True)
for i in range(100):
    print(i + 1, "\t", c[i])
