#!/usr/bin/env python3
import os
import sys
from tqdm import tqdm

vecfile = sys.argv[1]
print(f'Vector file : {vecfile}')
new_file = vecfile + '.upload'

with open(vecfile, 'r') as in_f, open(new_file, 'w') as out_f:
    num_items, dim = in_f.readline().strip().split()
    print(f'Num of items : {num_items}, dim : {dim}')
    
    for idx, line in tqdm(enumerate(in_f)):
        tmp = line.split()
        item_id = tmp[0]
        emb = tmp[1:]

        print(f"{idx}\t{item_id}\t{','.join(emb)}", file=out_f)

print(f'Save vector file : {new_file}')
