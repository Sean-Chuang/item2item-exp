#!/usr/bin/env python3
# session analysis
from collections import defaultdict
from tqdm import tqdm
from common import get_t
import os
import argparse
import time
import pandas as pd


def read_file(file_name, out_dir=None):
    colnames=['ad_id', 'item_id', 'behavior', 'ts'] 
    data = pd.read_csv(file_name, sep='\t', names=colnames, header=None)

    pop_df = data.groupby(['item_id', 'behavior']).size().reset_index(name='count')
    # save 2 pop in file
    header = ["item_id", "count"]
    pop_df.query('behavior == "ViewContent"').sort_values('count', ascending=False).to_csv(f'{out_dir}/items_view_freq.csv', columns=header, sep='\t', index=False, header=False)
    pop_df.query('behavior == "revenue"').sort_values('count', ascending=False).to_csv(f'{out_dir}/items_view_freq.csv', columns=header, sep='\t', index=False, header=False)

    # group user behavior
    data['combine'] = (data['behavior'] + ':' + data['item_id']).astype(str)
    header = ["combine"]
    data.sort_values(by='ts').groupby(['ad_id'])['combine'].agg(lambda x: ' '.join(x)).reset_index().to_csv(f'{out_dir}/tr_data.csv', columns=header, sep='\t', index=False, header=False)


if __name__ == '__main__':
    parser = argparse.ArgumentParser("python3 0_raw_data_handler.py")
    parser.add_argument("date", type=str, help="date")
    parser.add_argument("output_dir", type=str, help="output file foler")
    parser.add_argument("--session_period", type=int, default=None, help="how long would consider the new session (sec)")
    parser.add_argument("--last_N", type=int, default=10, help="How many reference events when testing")
    args = parser.parse_args()
    tr_data = f"data/{args.date}/tr_data/merged.data"
    te_data = f"data/{args.date}/te_data/merged.data"
    # train
    print(f"[{get_t()}] reading train data events")
    read_file(tr_data, args.output_dir)

    # print(f"[{get_t()}] reading sample data events")
    # sample_events = read_file('data/sample.csv', 'data')
    # sample_sessions, sample_last_N_events = session_process(sample_events, session_period=None)
    # user_events_session_statistic(sample_sessions)
    # save_user_event_seqence(sample_sessions, 'tr_data.csv')
    # save_test_file(sample_sessions, sample_last_N_events, 'te_data.csv')
    # # for u in sample_sessions:
    # #     print(sample_sessions[u])
