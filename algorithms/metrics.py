import numpy as np

def precision_recall(gt, pred):
    num_hit = len(gt & set(pred))
    precision = num_hit / (len(pred) if len(pred) != 0 else 1)
    recall = num_hit / (len(gt) if len(gt) != 0 else 1)
    if precision*recall == 0:
        f1 =  0 
    else: 
        f1 = 2 * precision * recall / (precision + recall)
    return precision , recall, f1


def average_precision(gt, pred):
    if not gt:
        return 0.0

    score = 0.0
    num_hits = 0.0
    for i,p in enumerate(pred):
        if p in gt and p not in pred[:i]:
            num_hits += 1.0
            score += num_hits / (i + 1.0)

    return score / max(1.0, len(gt))


def hit_rate(gt, pred):
    intersec = len(gt & set(pred))
    # return intersec / max(1, float(len(gt)))
    return intersec / len(gt)


def mrr(gt, pred, k=None):
    if k:
        _pred = pred[:k]
    else:
        _pred = pred

    score = 0.0
    for rank, item in enumerate(_pred):
        if item in gt:
            score = 1.0 / (rank + 1.0)
            break

    return score


def ndcg(gt, pred, use_graded_scores=False):
    score = 0.0
    for rank, item in enumerate(pred):
        if item in gt:
            if use_graded_scores:
                grade = 1.0 / (gt.index(item) + 1)
            else:
                grade = 1.0
            score += grade / np.log2(rank + 2)

    norm = 0.0
    for rank in range(len(gt)):
        if use_graded_scores:
            grade = 1.0 / (rank + 1)
        else:
            grade = 1.0
        norm += grade / np.log2(rank + 2)
    return score / max(0.3, norm)


def metrics(gt, pred, metrics_map):
    '''
    Returns a numpy array containing metrics specified by metrics_map.
    gt: set
            A set of ground-truth elements (order doesn't matter)
    pred: list
            A list of predicted elements (order does matter)
    '''
    out = [0] * len(metrics_map)

    if 'P&R' in metrics_map:
        precision, recall, f1 = precision_recall(gt, pred)
        out[metrics_map.index('P&R')] = [precision, recall, f1]

    if 'MAP' in metrics_map:
        avg_precision = average_precision(gt, pred)
        out[metrics_map.index('MAP')] = avg_precision

    if 'HR' in metrics_map:
        score = hit_rate(gt, pred)
        out[metrics_map.index('HR')] = score

    if 'MRR' in metrics_map:
        score = mrr(gt, pred)
        out[metrics_map.index('MRR')] = score

    if 'MRR@10' in metrics_map:
        score = mrr(gt, pred, k=10)
        out[metrics_map.index('MRR@10')] = score

    if 'NDCG' in metrics_map:
        score = ndcg(gt, pred)
        out[metrics_map.index('NDCG')] = score

    return out


if __name__ == '__main__':
    # test predict the next purchase item
    metrics_map = ['HR', 'MRR', 'MRR@10', 'NDCG']
    gt = set([3, 5])
    pred = [1,6,7,8,3,4]
    print(metrics(gt, pred, metrics_map))
    # test predict follow items
    metrics_map = ['P&R', 'MAP', 'MRR@10']
    gt = set([3,7,4,8])
    pred = [1,3,6,7,8,4,8,9,10]
    print(metrics(gt, pred, metrics_map))


