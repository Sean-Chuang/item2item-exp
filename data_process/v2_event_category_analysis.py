#!/usr/bin/env python3
import os
import time
import logging
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
log = logging.getLogger(__name__)

import pandas as pd
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
    return data

def get_user_activities_list(input_file, file_name='user_activies.csv', topK=500000):
    user_activities_count = defaultdict(int)
    if not os.path.exists(file_name):
        with open(file_name, 'r') as in_f:
            for line in tqdm(in_f):
                ad_id, item_id, behavior, ts = line.strip().split('\t')
                if item_id:
                    user_activities_count[ad_id] += 1
        data = pd.DataFrame(list(user_activities_count.items()), columns=['ad_id', 'count'])
        data = data.sort_values(by='count', ascending=False)
        data.to_csv(file_name, sep='\t')
    else:
        data = pd.read_csv(file_name, sep='\t')

    return data.head(topK)['ad_id'].tolist()


def get_user_sessions(data, topK_user):
    pass

def category_relation_analyasis():
    pass


def main():
    items_info = get_item_google_category()
    user_topK = get_user_activities_list("/mnt1/train/item2item-exp/data/2020-05-30/tr_data/merged.data")


if __name__ == '__main__':
    main()
