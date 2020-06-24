#!/usr/bin/env python3
import logging
import sys
from gensim.models import word2vec

def main():
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
    sentences = word2vec.LineSentence("/mnt1/train/item2item-exp/data/2020-05-30/tr_data.csv")
    model = word2vec.Word2Vec(sentences, size=50, alpha=0.025, sg=1, window=20, min_count=3, workers=20,)
    model.wv.save_word2vec_format("/mnt1/train/model/w2v.vec")

if __name__ == "__main__":
    main()