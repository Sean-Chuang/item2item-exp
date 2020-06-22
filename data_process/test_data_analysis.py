#!/usr/bin/env python3
import os, sys
import numpy as np
#file = '../data/2020-05-30/te_data.csv'
file='test.sample.14400.csv'
co_distinct_exist = []
co_exist = []
co_exist_item = []
co_idx = []
total = 0
exactly_same = 0
with open(file, 'r') as in_f:
    for line in in_f:
        history, predict = line.rstrip().split('\t')
        if history == '':
            continue
        # history_actions = set([e.split(':', 1)[0] for e in history.split('#')]) if e.split(':', 1)[0] == 'revenue'
        history_events = [e.split(':', 1)[1] for e in history.split('#') if e.split(':', 1)[0] != 'revenue']
        org_history_events = [e.split(':', 1)[1] for e in history.split('#')]
        predict_events = [e.split(':', 1)[1] for e in predict.split('#')[0:]]
        # predict_events = predict_events[:20]
        # if len(predict_events) < 5:
        #    continue
        total += 1
        co_items = set(history_events) & set(predict_events)
        if len(co_items) > 0:
            co_distinct_exist.append(len(co_items))
            co_idx.append(min([org_history_events[::-1].index(x) for x in co_items]))
            co_exist.append(len([x for x in history_events if x in predict_events]))
            co_exist_item.append([x for x in history_events if x in predict_events])
    print(f'co_distinct_exist:{np.mean(co_distinct_exist)}')
    print(f'co_exist:{np.mean(co_exist)}')
    print(f'co_idx:{np.mean(co_idx)}')
    # print(f'co_exist_item:{co_exist_item}')
    print(f'count: {len(co_exist)}, total: {total},  ratio : {len(co_exist) / total}')
