import os
import pickle

current_dir = os.path.dirname(__file__)

tmp_dir = os.path.join(current_dir, '..', "tmp")
os.makedirs(tmp_dir)

def save_pk(obj, filename):
    with open(f'{tmp_dir}/{filename}.pickle', 'wb') as handle:
        pickle.dump(a, handle, protocol=pickle.HIGHEST_PROTOCOL)


def load_pk(filename):
    with open(f'{tmp_dir}/{filename}.pickle', 'rb') as handle:
        obj = pickle.load(handle)
    return obj
