#!/usr/bin/env python3
import csv
import json
import itertools
import string

infile = 'short.schema'
states = [ "AK", "AL", "AR", "AZ", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "IA", "ID", "IL", "IN", "KS",
           "KY", "LA", "MA", "MD", "ME", "MI", "MN", "MO", "MS", "MT", "NC", "ND", "NE", "NH", "NJ", "NM",
           "NV", "NY", "OH", "OK", "OR", "PA", "PR", "RI", "SC", "SD", "TN", "TT", "TX", "UT", "VA", "VT",
           "WA", "WI", "WV", "WY" ]

def get_metadata(g, gMin, gMax):
    return {'type': "DoubleColumnQuantization",
            'granularity': g,
            'globalMin': gMin,
            'globalMax': gMax}

def get_string_metadata(col):
    letters = list(string.ascii_uppercase) + list(string.ascii_lowercase)
    if col == "OriginState" or col == "DestState":
        letters = states
    return {'type': "StringColumnQuantization",
            'globalMax': 'z',
            'leftBoundaries': letters }

def concat_colnames(colnames):
    return '+'.join(colnames)

def main():
    colnames = []
    with open(infile, 'r') as f:
        contents = "".join(line for line in f)
        schema = json.loads(contents)
        colnames = map(lambda s: s["name"], schema)

    length2 = itertools.combinations(colnames, 2)
    length2 = [sorted(x) for x in length2]

    with open('privacy_metadata.json', 'w') as f:
        quantization = {}
        epsilons = {}
        for col in schema:
            cn = col["name"]
            if col["kind"] == "String":
                quantization[cn] = get_string_metadata(cn)
            else:
                quantization[cn] = get_metadata(5.0, -100.0, 100.0)
            epsilons[cn] = 1
        for cn in length2:
            concat_cn = concat_colnames(cn)
            epsilons[concat_cn] = 0.1
        output = {'quantization': { 'quantization': quantization }, 'epsilons': epsilons }
        f.write(json.dumps(output))

if __name__=='__main__':
    main()