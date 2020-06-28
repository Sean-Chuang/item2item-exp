import os
import time
import logging
import pandas as pd

from pyhive import presto
from annoy import AnnoyIndex
from decimal import Decimal
from collections import OrderedDict
from enum import Enum, unique

import boto3
from boto3.dynamodb.conditions import Key

import json
from json import encoder
encoder.FLOAT_REPR = lambda o: format(o, '.4f')

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
log = logging.getLogger(__name__)

cursor = presto.connect('presto.smartnews.internal',8081).cursor()
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


def build_ann(items_vec_file, save_index=False):
    """
        Only save valid items in ann
    """
    b_time = time.time()
    item_idx_map = {}
    log.info("[build_ann] Start to read vectors")
    with open(items_vec_file, 'r') as in_f:
        num_items, dim = in_f.readline().strip().split()
        log.info(f"Num of items : {num_items}, dim : {dim}")
        ann_model = AnnoyIndex(int(dim), 'angular')
        
        for idx, line in enumerate(in_f):
            tmp = line.split()
            item_id = tmp[0]
            emb = list(map(float, tmp[1:]))
            item_idx_map[idx] = item_id
            ann_model.add_item(idx, emb)

    log.info("[build_ann] Start to build ann index")
    index_file = items_vec_file 
    ann_model.build(30)
    if save_index:
        ann_model.save(f"{index_file}.ann")

    log.info(f"[Time|build_ann] Cost : {time.time() - b_time}")
    return item_idx_map, ann_model


def fetch_topK_similar(ann_model, topK, item_idx_map, valid_items):
    """
    result : {
        "[behavior]" : {item_a: socre, item_b: score, }

    """
    b_time = time.time()
    topK_similar_result = {}
    for idx in item_idx_map:
        item_label = item_idx_map[idx]
        res_dict = OrderedDict()
        topK_item, topK_dist = ann_model.get_nns_by_item(idx, topK*3, include_distances=True)
        for item_idx, dist in zip(topK_item, topK_dist):
            try:
                item = item_idx_map[item_idx].split(':', 1)[1].strip()
                if item in valid_items and item not in res_dict:
                    res_dict[item] = Decimal(f"{1-dist:.4f}")
                    # Todo: maybe do score normalize here
            except Exception as err:
                log.error(err)
                log.warning(f"Couldn't find item name : {item_idx_map[item_idx]}")
            if len(res_dict) == topK:
                break
        topK_similar_result[item_label] = res_dict

    log.info(f"[Time|fetch_topK_similar] Cost : {time.time() - b_time}")
    return topK_similar_result


def insert_ddb(table_name, company_label, topK_similar):
    b_time = time.time()
    table = dynamodb.Table(table_name)
    # Step1: update the table data
    old_data = table.scan(FilterExpression=Key("label").eq(company_label),
                            ProjectionExpression="item_id"
                        )['Items']
    log.info(f"Old data size : {len(old_data)}")
    old_item = set()
    for item in old_data:
        old_item.add(item['item_id'])

    # prepare batch update data
    update_count = 0
    update_data = {}
    for e in topK_similar:
        try:
            action, item = e.split(':', 1)
            if item not in update_data:
                update_data[item] = {'label': company_label}
            _item = update_data[item]
            if action == Action.View.value:
                _item['view_similar'] = topK_similar[e]
            elif action == Action.AddToCart.value:
                _item['add_cart_similar'] = topK_similar[e]
            elif action == Action.Purchase.value:
                _item['purchase_similar'] = topK_similar[e]
            else:
                log.warning(f"{e} -> {action} not a valided action...")
                continue
            update_data[item] = _item
        except Exception as err:
            log.error(err)
            log.warning(f"{e} not a valided behaviors...")

    with table.batch_writer() as batch:
        for item in update_data:
            try:
                _item = update_data[item]
                _item['item_id'] = item
                batch.put_item(Item=_item)
                old_item.discard(item)
                update_count += 1
            except Exception as err:
                log.error(err)
                log.warning(f"{item} update failed...")

    # Step2: remove the old items
    log.info(f"Update items : {update_count}")
    log.info(f"Remove item size : {len(old_item)}")
    if update_count > 0:
        with table.batch_writer() as batch:
            for item in old_item:
                batch.delete_item(Key={'item_id':item, 'label':company_label})

    log.info(f"[Time|insert_ddb] Cost : {time.time() - b_time}")
    

def backup(file_name, topK_similar):
    b_time = time.time()
    # Save in file and upload s3.
    with open(file_name, 'w') as out_f:
        for item in topK_similar:
            print(f"{item}\t{json.dumps(topK_similar[item], cls=DecimalEncoder)}", file=out_f)

    log.info(f"[Time|backup] Cost : {time.time() - b_time}")


def main(company_table, items_vec_file, topK, ddb_table, company_label, backup_file):
    b_time = time.time()
    valid_items_set = fetch_category_items(company_table)
    item_idx_map, ann_model = build_ann(items_vec_file, True)
    topK_similar_result = fetch_topK_similar(ann_model, topK, item_idx_map, valid_items_set)
    insert_ddb(ddb_table, company_label, topK_similar_result)
    backup(backup_file, topK_similar_result)
    log.info(f"[Time|main] Cost : {time.time() - b_time}")


if __name__ == '__main__':
    company_data_table = "hive.maeda.rakuten_rpp_datafeed"
    items_vec_file = "../data/sample_model.vec"
    topK = 10
    ddb_table_name = "dev_dynamic_ads_similar_items"
    company_label = "rakuten_shopping"
    backup_file = "similar_items_res.csv"
    main(company_data_table, items_vec_file, topK, ddb_table_name, company_label, backup_file)

