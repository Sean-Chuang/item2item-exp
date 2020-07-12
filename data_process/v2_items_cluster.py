#!/usr/bin/env python3
import numpy as np
import pandas as pd
import pqkmeans
import sys
import pickle
import time
from tqdm import tqdm


def get_items_emb(vec_file):
    with open(vec_file, 'r') as in_f:
        num_items, dim = in_f.readline().strip().split()
        print(f'Num of items : {num_items}, dim : {dim}')
        vectors = np.empty((int(num_items), int(dim)), dtype=float)
        items = []
        for idx, line in tqdm(enumerate(in_f)):
            tmp = line.strip().split()
            items.append(tmp[0])
            vec = np.array(tmp[1:], dtype=float)
            norm = np.linalg.norm(vec)
            vectors[idx,:] = vec / norm

    return items, vectors



def cluster(data, k=2500):
    num, dim = data.shape
    print(num, dim)
    encoder = pqkmeans.encoder.PQEncoder(num_subdim=4, Ks=256)
    encoder.fit(data[:int(num*0.25)])
    X_pqcode = encoder.transform(data)
    # X_pqcode = encoder.transform_generator(data)

    # Run clustering
    kmeans = pqkmeans.clustering.PQKMeans(encoder=encoder, k=k)
    clustered = kmeans.fit_predict(X_pqcode)

    return clustered


def main():
    b_time = time.time()
    items, vecotrs = get_items_emb('/mnt1/train/model/w2v_cbow_64_w10_view.vec')
    clustered = cluster(vecotrs)
    print('Cost : ', time.time() - b_time)
    pd.DataFrame(list(zip(items, clustered)), columns=['item','label']).to_csv('./items_cluster_id.csv', sep='\t', index=False, header=False)
    

if __name__ == '__main__':
    main()
