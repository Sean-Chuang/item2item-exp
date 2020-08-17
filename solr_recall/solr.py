#!/usr/bin/env python3

import pysolr

class i2i_solr:

    def __init__(self, host, core):
        self.solr = pysolr.Solr(f"http://{host}/solr/{core}", timeout=10)

    def insert(self, data_list):
        self.solr.add(data_list)

    def search(self, history_list):
        h_string = ' '.join(history_list)
        res = self.solr.search("session:h_string")
        return res



def main(file_name, session=3600):
    solr_handler = i2i_solr('127.0.0.1:8983', 'i2i_demo')
    dt = '2020-08-01'
    with open(file_name, 'r') as in_f:
        for line in in_f:
            user, behaviors = line.strip().split('\t')
            user_data = []
            behavior_list = [tuple(items.split('@')) for items in behaviors.split(',')]
            sess_data = []
            start_idx = [0]
            for index, (_item_id, _type, ts) in enumerate(behavior_list):
                sess_data.append(_item_id)
                if index == 0:  continue
                if int(ts) - int(behavior_list[index-1][-1]) >= session:
                    start_idx.append(index)
                
            start_idx.append(len(sess_data))
            print(start_idx)
            print(sess_data)
            for idx in range(len(start_idx)-1):
                data = {
                    'id': f"{user}_{dt}_{len(user_data)}",
                    'uid': user,
                    'session': ' '.join(sess_data[start_idx[idx]:start_idx[idx+1]]),
                    'date': dt
                }
                user_data.append(data)

            print(user_data)
            solr_handler.insert(user_data)
            break




if __name__ == '__main__':
    data_f = "data/test.csv"
    main(data_f)
