#!/usr/bin/env python3
import os
from collections import defaultdict
from tqdm import tqdm
import pickle
import networkx as nx
from walker import RandomWalker

MIN_SESSION_NUM = 2

def get_user_session(f_name, session=3600):
    all_session = []
    with open(file_name, 'r') as in_f:
        for i, line in tqdm(enumerate(in_f)):
            user, behaviors = line.strip().split('\t')
            behavior_list = [tuple(items.split('@')) for items in behaviors.split(',')]
            sess_data = []
            start_idx = [0]
            for index, (_item_id, _type, ts) in enumerate(behavior_list):
                sess_data.append(_item_id.replace(':', '_'))
                if index == 0:  continue
                if int(ts) - int(behavior_list[index-1][-1]) >= session:
                    start_idx.append(index)
                
            start_idx.append(len(sess_data))
            for idx in range(len(start_idx)-1):
                sess = sess_data[start_idx[idx]:start_idx[idx+1]]
                if len(sess) <= MIN_SESSION_NUM:
                    continue

                all_session.append(sess)

    print(f'Total session : {len(all_session)}')
    return all_session


def random_walk(args):
    # make session (right now each user is one session)
    print('make session list\n')
    start_time = time.time()
    session_list_all = get_user_session(args.data_path)

    print('make session list done, time cost {0}'.format(str(time.time() - start_time)))

    # session to graph
    node_pair = defaultdict(int)
    for session in session_list_all:
        for i in range(1, len(session)):
            node_pair[(session[i - 1], session[i])] += 1

    in_node_list = list(map(lambda x: x[0], list(node_pair.keys())))
    out_node_list = list(map(lambda x: x[1], list(node_pair.keys())))
    weight_list = list(node_pair.values())
    graph_df = pd.DataFrame({'in_node': in_node_list, 'out_node': out_node_list, 'weight': weight_list})
    graph_df.to_csv('./data_cache/graph.csv', sep=' ', index=False, header=False)

    G = nx.read_edgelist('./data_cache/graph.csv', create_using=nx.DiGraph(), nodetype=None, data=[('weight', int)])
    walker = RandomWalker(G, p=args.p, q=args.q)
    print("Preprocess transition probs...")
    walker.preprocess_transition_probs()

    session_reproduce = walker.simulate_walks(num_walks=args.num_walks, walk_length=args.walk_length, workers=4,
                                              verbose=1)
    session_reproduce = list(filter(lambda x: len(x) > 2, session_reproduce))
    ptint(session_reproduce[:10])
    with open('test.data', 'w') as out_f:
        for sess in session_reproduce:
            out_f.write(' '.join(sess))



if __name__ == '__main__':
    # dt = '2020-08-01'
    # data_f = f"../solr_recall/data/{dt}/merged.data"

    parser = argparse.ArgumentParser(description='manual to this script')
    parser.add_argument("--data_path", type=str, default='./data/')
    parser.add_argument("--p", type=float, default=0.25)
    parser.add_argument("--q", type=float, default=2)
    parser.add_argument("--num_walks", type=int, default=10)
    parser.add_argument("--walk_length", type=int, default=10)

    args = parser.parse_known_args()[0]
    random_walk(args)

