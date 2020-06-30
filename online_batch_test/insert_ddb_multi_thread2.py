import os
import time
import logging
import pandas as pd

from pyhive import presto
from annoy import AnnoyIndex
from decimal import Decimal
from collections import OrderedDict, defaultdict
from enum import Enum, unique

import boto3
from boto3.dynamodb.conditions import Key

import json
from json import encoder
encoder.FLOAT_REPR = lambda o: format(o, '.4f')

import threading, queue

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
log = logging.getLogger(__name__)

cursor = presto.connect('presto.smartnews.internal',8081).cursor()
ddb_client = boto3.client('dynamodb', region_name='ap-northeast-1')
dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-1')

# enum
@unique
class Action(Enum):
    # View = "View"
    # AddToCart = "AddToCart"
    # Purchase = "Purchase"
    View = "ViewContent"
    AddToCart = "AddToCart"
    Purchase = "revenue"


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)


q = queue.Queue()

def insert_ddb(table_name, company_label):
    table = dynamodb.Table(table_name)
    update_count = 0

    with table.batch_writer() as batch:
        while True:
            item = q.get()
            try:
                _item = update_data[item]
                batch.put_item(Item=_item)
                update_count += 1
            except Exception as err:
                log.error(err)
                log.warning(f"{item} update failed...")
            if update_count % 100000 == 0:
                log.info(f'Finished upload items : {update_count}')
            q.task_done()


def __query_presto(query, limit=None):
    if limit:
        query += f" limit {limit}"

    cursor.execute(query)
    column_names = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(cursor.fetchall(), columns=column_names)
    return df


def __update_table_WCU(table, read_capacity, write_capacity):
    try:
        res = ddb_client.update_table(
            TableName=table, 
            ProvisionedThroughput={
                'ReadCapacityUnits': read_capacity,
                'WriteCapacityUnits': write_capacity
            }
        )
        log.info(f"Finish update the table WCU. [{read_capacity}, {write_capacity}]")
    except Exception as err:
        log.error(err)
        log.warning(f"WCU do not change...")


def fetch_category_items(company_table):
    valid_items = set()
    b_time = time.time()
    log.info("[fetch_category_items] Start query table...")
    query = f"""
        select 
            regexp_replace(id,'^([0-9]+):([0-9a-zA-Z\-_]+):([0-9]+)$','$2:$3') as content_id
        from {company_table} 
    """
    data = __query_presto(query)
    valid_items = set(data['content_id'].unique())
    log.info(f"Total valid_items counts : {len(valid_items)}")
    log.info(f"[Time|fetch_category_items] Cost : {time.time() - b_time}")
    return valid_items


def build_ann(items_vec_file, valid_items):
    """
        Only save valid items in ann
    """
    b_time = time.time()
    item_idx_map = {}
    item_group = defaultdict(list)
    valid_items_cnt = 0
    log.info("[build_ann] Start to read vectors")
    with open(items_vec_file, 'r') as in_f:
        num_items, dim = in_f.readline().strip().split()
        log.info(f"Num of items : {num_items}, dim : {dim}")
        ann_model = AnnoyIndex(int(dim), 'angular')
        
        for idx, line in enumerate(in_f):
            tmp = line.split()
            item_id = tmp[0]
            emb_str = tmp[1:]
            try:
                action, content_id = item_id.split(':', 1)
                item_idx_map[idx] = item_id
                item_group[content_id].append(item_id)
                if content_id in valid_items:
                    emb = list(map(float, emb_str))
                    ann_model.add_item(idx, emb)
                    valid_items_cnt += 1
            except Exception as err:
                log.error(err)
                log.warning(f"{item_id} not a valided behaviors...")

    log.info(f"[build_ann] Start to build ann index, total : {valid_items_cnt}")
    index_file = f"{items_vec_file}.ann"
    ann_model.build(30)
    ann_model.save(index_file)
    log.info(f"[Time|build_ann] Cost : {time.time() - b_time}")
    return ann_model, item_idx_map, item_group


def fetch_topK_similar(items_vec_file, ann_model, topK, item_idx_map, items_group):
    b_time = time.time()
    log.info("[fetch_topK_similar] Start to get topK items")
    update_data = {}
    with open(items_vec_file, 'r') as in_f:
        num_items, dim = in_f.readline().strip().split()
        for idx, line in enumerate(in_f):
            tmp = line.split()
            item_id = tmp[0]
            action, content_id = item_id.split(':', 1)
            items_group[content_id].remove(item_id)
            item_emb = list(map(float, tmp[1:]))
            if item_label not in update_data:
                update_data[item_label] = {'item_id': item_label, 'label': company_label}

            res_dict = OrderedDict()
            topK_item, topK_dist = ann_model.get_nns_by_vector(item_emb, topK*3, include_distances=True)
            for item_idx, dist in zip(topK_item, topK_dist):
                try:
                    item = item_idx_map[item_idx].split(':', 1)[1].strip()
                    if item not in res_dict:
                        res_dict[item] = Decimal(f"{1-dist:.4f}")
                        # Todo: maybe do score normalize here
                except Exception as err:
                    log.error(err)
                    log.warning(f"Couldn't find item name : {item_idx_map[item_idx]}")
                if len(res_dict) == topK:
                    break

            if action == Action.View.value:
                update_data[item_label]['view_similar'] = res_dict
            elif action == Action.AddToCart.value:
                update_data[item_label]['add_cart_similar'] = res_dict
            elif action == Action.Purchase.value:
                update_data[item_label]['purchase_similar'] = res_dict
            else:
                log.warning(f"{e} -> {action} not a valided action...")
                continue

            if len(items_group[content_id]) == 0:
                q.put(update_data[item_label])

    log.info(f"Check items group result : {sum([len(g) for g in items_group])}")
    log.info(f"[Time|fetch_topK_similar] Cost : {time.time() - b_time}")
    return update_data


def insert_ddb(table_name, company_label, update_data):
    b_time = time.time()
    log.info("[insert_ddb] Start to insert data in DDB")
    table = dynamodb.Table(table_name)

    # prepare batch update data
    update_count = 0
    with table.batch_writer() as batch:
        for item in update_data:
            try:
                _item = update_data[item]
                batch.put_item(Item=_item)
                update_count += 1
            except Exception as err:
                log.error(err)
                log.warning(f"{item} update failed...")

    log.info(f"Update items : {update_count}")
    log.info(f"[Time|insert_ddb] Cost : {time.time() - b_time}")
    

def backup(file_name, topK_similar):
    b_time = time.time()
    # Save in file and upload s3.
    log.info("[backup] Start to backup data")
    with open(file_name, 'w') as out_f:
        for item in topK_similar:
            print(f"{item}\t{json.dumps(topK_similar[item], cls=DecimalEncoder)}", file=out_f)

    log.info(f"[Time|backup] Cost : {time.time() - b_time}")


def main(company_table, items_vec_file, topK, ddb_table, company_label, backup_file):
    b_time = time.time()
    valid_items_set = fetch_category_items(company_table)
    ann_model, item_idx_map, item_group = build_ann(items_vec_file, valid_items_set)
    topK_similar_result = fetch_topK_similar(items_vec_file, ann_model, topK, item_idx_map, item_group)
    # insert_ddb(ddb_table, company_label, topK_similar_result)
    # backup(backup_file, topK_similar_result)
    log.info(f"[Time|main] Cost : {time.time() - b_time}")


if __name__ == '__main__':
    company_data_table = "hive.maeda.rakuten_rpp_datafeed"
    items_vec_file = "../data/sample_model.vec"
    topK = 10
    ddb_table_name = "dev_dynamic_ads_similar_items"
    company_label = "rakuten_shopping"
    backup_file = "similar_items_res.csv"
    threading.Thread(target=worker, args=(ddb_table_name, company_label), daemon=True).start() 
    main(company_data_table, items_vec_file, topK, ddb_table_name, company_label, backup_file)

