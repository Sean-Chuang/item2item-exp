#!/usr/bin/env python3
# session analysis
from collections import defaultdict
from tqdm import tqdm
from common import get_t
import os
import argparse
import time

def read_file(file_name, out_dir=None):
    user_events = defaultdict(list)
    items_view_freq = defaultdict(int)
    items_purchase_freq = defaultdict(int)
    item_list = set()
    behavior_list = set()
    with open(file_name, 'r') as in_f:
        for line in tqdm(in_f):
            ad_id, item_id, behavior, ts = line.strip().split('\t')
            if item_id:
                item_id = item_id.replace(' ', '')
                user_events[ad_id].append((item_id, behavior, int(ts)))
                if behavior == 'ViewContent':
                    items_view_freq[item_id] += 1
                elif behavior == 'revenue':
                    items_purchase_freq[item_id] += 1
                item_list.add(item_id)
                behavior_list.add(behavior + ':' + item_id)
    if out_dir:
        with open(f'{out_dir}/items_view_freq.csv', 'w') as f:
            [print(f'{key}\t{value}', file=f) for key, value in sorted(items_view_freq.items(), key=lambda item: item[1], reverse=True)]
        with open(f'{out_dir}/items_purchase_freq.csv', 'w') as f:
            [print(f'{key}\t{value}', file=f) for key, value in sorted(items_purchase_freq.items(), key=lambda item: item[1], reverse=True)]

        with open(f'{out_dir}/user_idx.csv', 'w') as f:
            [print(f'{key}\t{idx}', file=f) for idx, key in enumerate(list(user_events.keys()))]
        with open(f'{out_dir}/item_idx.csv', 'w') as f:
            [print(f'{key}\t{idx}', file=f) for idx, key in enumerate(list(item_list))]
        with open(f'{out_dir}/behavior_idx.csv', 'w') as f:
            [print(f'{key}\t{idx}', file=f) for idx, key in enumerate(list(behavior_list))]
    return user_events

# session_period : sec
def session_process(tmp_user_events, session_period=None, last_N=10):
    user_sessions = defaultdict(list)
    user_last_N_events = defaultdict(list)
    user_events = defaultdict(list)
    for ad_id in tqdm(tmp_user_events):
        if len(tmp_user_events[ad_id]) > 5:
                user_events[ad_id] = tmp_user_events[ad_id]

    if session_period:
        for ad_id in tqdm(user_events):
            tmp = []
            user_events[ad_id].sort(key = lambda x:x[-1])
            user_last_N_events[ad_id] = user_events[ad_id][-last_N:]
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
            user_last_N_events[ad_id] = user_events[ad_id][-last_N:]
            user_sessions[ad_id].append(user_events[ad_id])

    return user_sessions, user_last_N_events


def user_events_session_statistic(user_event_session):
    user_count = len(user_event_session)
    sessions_count, session_length = 0, 0
    for idx, user in tqdm(enumerate(user_event_session)):
        sessions = user_event_session[user]
        if len(sessions) < 2:
            continue
        # sessions_count += len(sessions)
        for session in sessions:
            session_length += len(session)
            sessions_count += 1
        if idx < 1000:
            print(f"{user} ---> {user_event_session[user]}")
    print(f'user_count:{user_count}, sessions_count:{sessions_count}, avg_session_length:{session_length/sessions_count}')


def save_test_file_new(user_event_session , file_name, last_N=10):
    with open(file_name, 'w') as out_f:
        for user in tqdm(user_event_session):
            sessions = user_event_session[user]
            if len(sessions) < 2:
                continue
            
            for idx in range(len(sessions)-1):
                if len(sessions[idx]) >= last_N and len(sessions[idx+1]) >= 5:
                    history_events = [f'{s[1]}:{s[0]}' for s in sessions[idx]]
                    predict_events = [f'{s[1]}:{s[0]}' for s in sessions[idx+1]]
                    if set(history_events) != set(predict_events):
                        print(f"{'#'.join(history_events)}\t{'#'.join(predict_events)}", file=out_f)

 
def save_user_event_seqence(user_event_session, file_name):
    """
    - train seqence
    - test prefix 10 seqence
    """
    with open(file_name, 'w') as out_f:    
        for user in tqdm(user_event_session):
            for session in user_event_session[user]:
                if len(session) < 3:    continue
                events = [f'{s[1]}:{s[0]}' for s in session]
                print(' '.join(events), file=out_f)
                


def save_test_file(user_event_session, user_last_N_events, file_name):
    """
    ev1, ev2, ev3, ev4, ev5 ......
    ev6, ev7, ev8, ev9, ......
    """
    with open(file_name, 'w') as out_f:    
        for user in tqdm(user_event_session):
            if user in user_last_N_events:
                print('#'.join([f'{s[1]}:{s[0]}' for s in user_last_N_events[user]]), end='\t', file=out_f)
            else:
                print('', end='\t', file=out_f)

            for session in user_event_session[user]:
                events = [f'{s[1]}:{s[0]}' for s in session]
                print('#'.join(events), file=out_f)
            



if __name__ == '__main__':
    parser = argparse.ArgumentParser("python3 0_raw_data_handler.py")
    parser.add_argument("date", type=str, help="date")
    parser.add_argument("output_dir", type=str, help="output file foler")
    parser.add_argument("--session_period", type=int, default=None, help="how long would consider the new session (sec)")
    parser.add_argument("--last_N", type=int, default=10, help="How many reference events when testing")
    args = parser.parse_args()
    tr_data = f"data/{args.date}/tr_data/merged.data"
    te_data = f"../data/{args.date}/te_data/merged.data"
    #te_data = f"te_sample.data"
    # train
    print(f"[{get_t()}] reading train data events")
    #events = read_file(tr_data, args.output_dir)
    print(f"[{get_t()}] train data session_process")
    #sessions, last_N_events = session_process(events, session_period=args.session_period, last_N=args.last_N)
    print(f"[{get_t()}] train data session_statistic")
    #user_events_session_statistic(sessions)
    print(f"[{get_t()}] train data save file")
    #save_user_event_seqence(sessions, os.path.join(args.output_dir, 'tr_data.csv'))
    # release memory
    events, sessions = None, None
    #time.sleep(15)

    # test
    print(f"[{get_t()}] reading test data events")
    events = read_file(te_data)
    print(f"[{get_t()}] test data session_process")
    sessions, _ = session_process(events, session_period=args.session_period, last_N=args.last_N)
    print(f"[{get_t()}] test data session_statistic")
    user_events_session_statistic(sessions)
    save_test_file_new(sessions, f'test.sample.{args.session_period}.csv', last_N=args.last_N)

    # print(f"[{get_t()}] reading sample data events")
    # sample_events = read_file('data/sample.csv', 'data')
    # sample_sessions, sample_last_N_events = session_process(sample_events, session_period=None)
    # user_events_session_statistic(sample_sessions)
    # save_user_event_seqence(sample_sessions, 'tr_data.csv')
    # save_test_file(sample_sessions, sample_last_N_events, 'te_data.csv')
    # # for u in sample_sessions:
    # #     print(sample_sessions[u])
