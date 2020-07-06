#!/usr/bin/env python3
import os
import time
import logging
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
log = logging.getLogger(__name__)

import pandas as pd
import numpy as np
from tqdm import tqdm
from collections import defaultdict
from pyhive import presto
cursor = presto.connect('presto.smartnews.internal',8081).cursor()

def __query_presto(query, limit=None):
    if limit:
        query += f" limit {limit}"

    cursor.execute(query)
    column_names = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(cursor.fetchall(), columns=column_names)
    return df

def get_item_google_category(file_name='items_info.csv'):
    b_time = time.time()
    log.info("[fetch_category_items] Start query table...")
    query = f"""
        select 
            replace(regexp_replace(id,'^([0-9]+):([0-9a-zA-Z\-_]+):([0-9]+)$','$2:$3'), ' ') as content_id, 
            title, 
            google_product_category, 
            try_cast(regexp_replace(price, 'JPY', '') as double) as price 
        from hive.maeda.rakuten_rpp_datafeed 
    """
    if not os.path.exists(file_name):
        data = __query_presto(query)
        data.to_csv(file_name, sep='\t')
        log.info(f"Total category items counts : {len(data.index)}")
    else:
        data = pd.read_csv(file_name, sep='\t')
    log.info(f"[Time|fetch_category_items] Cost : {time.time() - b_time}")
    data = data.set_index('content_id')
    res = data['google_product_category'].T.to_dict()
    return res

def get_user_events(input_file, activies_file_name='user_activies.csv', topK=500000):
    user_activities_count = defaultdict(int)
    if not os.path.exists(activies_file_name):
        with open(input_file, 'r') as in_f:
            for line in tqdm(in_f):
                ad_id, item_id, behavior, ts = line.strip().split('\t')
                if item_id:
                    user_activities_count[ad_id] += 1
        data = pd.DataFrame(list(user_activities_count.items()), columns=['ad_id', 'count'])
        data = data.sort_values(by='count', ascending=False)
        data.to_csv(activies_file_name, sep='\t')
    else:
        data = pd.read_csv(activies_file_name, sep='\t')
    user_topK_list = set(data.head(topK)['ad_id'].tolist())

    user_events = defaultdict(list)
    with open(input_file, 'r') as in_f:
        for line in tqdm(in_f):
            ad_id, item_id, behavior, ts = line.strip().split('\t')
            if item_id and ad_id in user_topK_list:
                user_events[ad_id].append((item_id, behavior, int(ts)))
    log.info(f"Total user : {len(user_events)}")
    return user_events


def get_user_sessions(user_events, session_period=3600):
    user_sessions = defaultdict(list)
    if session_period:
        for ad_id in tqdm(user_events):
            tmp = []
            user_events[ad_id].sort(key = lambda x:x[-1])
            for event in user_events[ad_id]:
                if len(tmp) == 0:
                    tmp.append(event)
                else:
                    if event[-1] - tmp[-1][-1] < session_period:
                        tmp.append(event)
                    else:
                        user_sessions[ad_id].append(tmp)
                        tmp = [event]
            if len(tmp) > 0:
                user_sessions[ad_id].append(tmp)

    else:
        for ad_id in tqdm(user_events):
            user_events[ad_id].sort(key = lambda x:x[-1])
            user_sessions[ad_id].append(user_events[ad_id])

    return user_sessions


def category_relation_analyasis(user_session, items_cat_info):
    viewed_cat_in_next_if_purchased = []
    viewed_cat_in_next_if_not_purchased = []
    purchase_cat_in_next_if_purchased = []
    new_cat_in_next_if_purchased = []
    new_cat_in_next_if_not_purchased = []
    for ad_id in tqdm(user_session):
        user_data = []
        for sess in user_session[ad_id]:
            sess_data = {'purchase':set(), 'viewed':set()}
            for item_id, behavior, ts in sess:
                if item_id in items_cat_info:
                    category = items_cat_info[item_id]
                    if behavior == 'revenue':
                        sess_data['purchase'].add(category)
                    else:
                        sess_data['viewed'].add(category)
            user_data.append(sess_data)

        for i in range(len(user_data)-1):
            now_session = user_data[i]
            next_session = user_data[i+1]
            next_total_cat = next_session['purchase'] | next_session['viewed']

            new_cat = next_total_cat - (now_session['purchase'] | now_session['viewed'])
            viewed_cat_in_next = (now_session['viewed'] - now_session['purchase']) & next_total_cat
            purchase_cat_in_next = now_session['purchase'] & next_total_cat
            if len(now_session['purchase']) > 0:
                viewed_cat_in_next_if_purchased.append(len(viewed_cat_in_next))
                purchase_cat_in_next_if_purchased.append(len(purchase_cat_in_next))
                new_cat_in_next_if_purchased.append(len(new_cat))
            else:
                new_cat_in_next_if_not_purchased.append(len(new_cat))
                viewed_cat_in_next_if_not_purchased.append(len(viewed_cat_in_next))

    print(f"new_cat_in_next_if_purchased: {np.mean(new_cat_in_next_if_purchased)}\nnew_cat_in_next_if_not_purchased:{np.mean(new_cat_in_next_if_not_purchased)}")
    print(f"viewed_cat_in_next_if_purchased: {np.mean(viewed_cat_in_next_if_purchased)}\nviewed_cat_in_next_if_not_purchased:{np.mean(viewed_cat_in_next_if_not_purchased)}")
    print(f"purchase_cat_in_next_if_purchased: {np.mean(purchase_cat_in_next_if_purchased)}")


def main():
    items_cat_info = get_item_google_category()
    user_events = get_user_events("/mnt1/train/item2item-exp/data/2020-05-30/tr_data/merged.data")
    user_sessions = get_user_sessions(user_events, session_period=3600)
    category_relation_analyasis(user_sessions, items_cat_info)

if __name__ == '__main__':
    main()
