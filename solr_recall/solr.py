#!/usr/bin/env python3
from tqdm import tqdm
import pysolr

class i2i_solr:

    def __init__(self, host, core):
        self.solr = pysolr.Solr(f"http://{host}/solr/{core}", timeout=10)

    def insert(self, data_list, commit=False):
        self.solr.add(data_list, commit)

    def search(self, history_list):
        history_list = [item.replace(':', '_') for item in history_list]
        h_string = ' '.join(history_list)
        res = self.solr.search(h_string, fl='session,score', rows=200)
        return res

    def commit(self):
        self.solr.commit()

    def delete_all(self):
        self.solr.delete(q='*:*')


solr_handler = i2i_solr('127.0.0.1:8983', 'i2i_demo')
MIN_SESSION_NUM = 3

def main(file_name, dt, session=3600):
    solr_handler.delete_all()
    with open(file_name, 'r') as in_f:
        for i, line in tqdm(enumerate(in_f)):
            user, behaviors = line.strip().split('\t')
            user_data = []
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
                sess = set(sess_data[start_idx[idx]:start_idx[idx+1]])
                if len(sess) <= MIN_SESSION_NUM:
                    continue

                data = {
                    'id': f"{user}_{dt}_{len(user_data)}",
                    'uid': user,
                    'session': ' '.join(sess),
                    'date': dt
                }
                user_data.append(data)

            if user_data:
                solr_handler.insert(user_data)

            if i!=0 and i%100000 == 0:
                solr_handler.commit()
                print(f"finished totol user {i}")


if __name__ == '__main__':
    # dt = '2020-08-01'
    # data_f = f"data/{dt}/merged.data"
    # main(data_f, dt)
    result = solr_handler.search(["okoku:11498247", "okoku:11499222", "okoku:11485043"])
    print(result.raw_response['response']['docs'])
