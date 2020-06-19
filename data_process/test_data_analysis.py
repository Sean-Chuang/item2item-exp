#!/usr/bin/env python3
import os
import numpy as np
file = '../data/te_sample.csv'
co_distinct_exist = []
co_exist = []
co_exist_item = []
total = 0
with open(file, 'r') as in_f:
    for line in in_f:
        history, predict = line.rstrip().split('\t')
        if history == '':
            continue
        total += 1
        history_events = [e.split(':', 1)[1] for e in history.split('#')]
        predict_events = [e.split(':', 1)[1] for e in predict.split('#')]
        if len(set(history_events) & set(predict_events)) > 1:
            co_distinct_exist.append(len(set(history_events) & set(predict_events)))
            co_exist.append(len([x for x in history_events if x in predict_events]))
            co_exist_item.append([x for x in history_events if x in predict_events])
    print(f'co_distinct_exist:{np.mean(co_distinct_exist)}')
    print(f'co_exist:{np.mean(co_exist)}')
    # print(f'co_exist_item:{co_exist_item}')
    print(f'ratio : {len(co_distinct_exist) / total}')