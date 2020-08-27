import os
from collections import defaultdict
from tqdm import tqdm
import pickle

def get_user_behavir(f_name):
    res = defaultdict(set)
    with open(f_name, 'r') as in_f:
        for line in tqdm(in_f):
            user, behaviors = line.strip().split('\t')
            item_set = set([items.split('@')[0] for items in behaviors.split(',')])
            res[user] = set(item_set)

    return res


def swing(user_items):
    u2Swing = defaultdict(dict)

    for u in tqdm(user_items):
        wu = pow(len(user_items[u])+5, -0.35)
        for v in user_items:
            if v == u:
                continue
            wv = pow(len(user_items[v])+5, -0.35)
            inter_items = user_items[u]&user_items[v]
            for i in inter_items:
                for j in inter_items:
                    if j == i:
                        continue
                    if j not in user_items[i]:
                        user_items[i][j] = 0
                    user_items[i][j] += (wu * wv / (1 + len(inter_items)))

    return u2Swing


def main():



if __name__ == '__main__':
    data_f = "data/test.csv"
    user_behavir = get_user_behavir(data_f)
    sim_res = swing(user_behavir)
    pickle.dump(sim_res, open( "swing_sim.pkl", "wb" ))
