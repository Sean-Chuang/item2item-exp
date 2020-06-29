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

from joblib import Parallel, delayed

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
log = logging.getLogger(__name__)

cursor = presto.connect('127.0.0.1',8081).cursor()
ddb_client = boto3.client('dynamodb')
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
    data = __query_presto(query, 1000)
    valid_items = set(data['content_id'].unique())
    log.info(f"Total valid_items counts : {len(valid_items)}")
    log.info(f"[Time|fetch_category_items] Cost : {time.time() - b_time}")
    return valid_items


def build_ann(items_vec_file, valid_items):
    """
        Only save valid items in ann
    """
    b_time = time.time()
    item_idx_map, context_emb_data = {}, defaultdict(dict)
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
                context_emb_data[content_id][action] = emb_str
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
    context_emb_data = [(k, v) for k, v in context_emb_data.items()] 
    log.info(f"[Time|build_ann] Cost : {time.time() - b_time}")
    return item_idx_map, context_emb_data, index_file, int(dim)


def multi_task(ann_model_file, dim, item_idx_map, context_emb_data, topK, ddb_table, company_label, jobs=5):
    b_time = time.time()
    log.info("[multi_task] Start to multi_task")
    __update_table_WCU(ddb_table, 20, 500)

    batch_size = 25
    Parallel(n_jobs=jobs, backend="multiprocessing")(
        delayed(fetch_topK_similar)(ann_model_file, dim, topK, item_idx_map, context_emb_data[i:i+batch_size], ddb_table, company_label) for i in range(0, len(context_emb_data), batch_size)
    )
    __update_table_WCU(ddb_table, 5, 2)
    log.info(f"[Time|multi_task] Cost : {time.time() - b_time}")


def fetch_topK_similar(ann_model_file, dim, topK, item_idx_map, items_vec_batch, ddb_table, company_label):
    b_time = time.time()
    log.info("[fetch_topK_similar] Start to get topK items")
    ann_model = AnnoyIndex(dim, 'angular')
    ann_model.load(ann_model_file)
    update_data = {}
    for item_label, action_vecs in items_vec_batch:
        update_data[item_label] = {'item_id': item_label, 'label': company_label}
        for action in action_vecs:    
            item_emb = list(map(float, action_vecs[action]))
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

    log.info(f"[Time|fetch_topK_similar] Cost : {time.time() - b_time}")
    
    insert_ddb(ddb_table, company_label, update_data)


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
    item_idx_map, context_emb_data, ann_model_file, dim = build_ann(items_vec_file, valid_items_set)
    multi_task(ann_model_file, dim, item_idx_map, context_emb_data, topK, ddb_table, company_label)
    # topK_similar_result = fetch_topK_similar(ann_model, topK, item_idx_map, item_idx_vector)
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
    main(company_data_table, items_vec_file, topK, ddb_table_name, company_label, backup_file)

