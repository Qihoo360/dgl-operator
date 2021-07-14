import os
import urllib.request as ur
import zipfile
import errno

import dgl
import numpy as np
import torch as th
import argparse
import time


def extract_zip(path, folder):
    with zipfile.ZipFile(path, 'r') as f:
        print('Extract', path)
        f.extractall(folder)

def makedirs(path):
    try:
        os.makedirs(os.path.expanduser(os.path.normpath(path)))
    except OSError as e:
        if e.errno != errno.EEXIST and os.path.isdir(path):
            raise e

def load_dataset(name, work_dir, url):
    from ogb.nodeproppred import DglNodePropPredDataset

    download_url(url, work_dir)
    extract_zip(f'{work_dir}/ogbn_products.zip', work_dir)
    os.remove(f'{work_dir}/ogbn_products.zip')
    
    print('load', name)
    data = DglNodePropPredDataset(name=name, root=work_dir)
    print('finish loading', name)
    splitted_idx = data.get_idx_split()
    graph, labels = data[0]
    labels = labels[:, 0]

    graph.ndata['features'] = graph.ndata['feat']
    graph.ndata['labels'] = labels
    in_feats = graph.ndata['features'].shape[1]
    num_labels = len(th.unique(labels[th.logical_not(th.isnan(labels))]))

    # Find the node IDs in the training, validation, and test set.
    train_nid, val_nid, test_nid = splitted_idx['train'], splitted_idx['valid'], splitted_idx['test']
    train_mask = th.zeros((graph.number_of_nodes(),), dtype=th.bool)
    train_mask[train_nid] = True
    val_mask = th.zeros((graph.number_of_nodes(),), dtype=th.bool)
    val_mask[val_nid] = True
    test_mask = th.zeros((graph.number_of_nodes(),), dtype=th.bool)
    test_mask[test_nid] = True
    graph.ndata['train_mask'] = train_mask
    graph.ndata['val_mask'] = val_mask
    graph.ndata['test_mask'] = test_mask
    print('finish constructing', name)
    return graph, num_labels

def download_url(url, folder):
    filename = url.rpartition('/')[-1]
    path = os.path.join(folder, filename)
    print('Download', url)
    makedirs(folder)
    data = ur.urlopen(url)
    size = int(data.info()["Content-Length"])
    chunk_size = 1024*1024
    num_iter = int(size/chunk_size) + 2
    downloaded_size = 0
    try:
        with open(path, 'wb') as f:
            for i in range(num_iter):
                chunk = data.read(chunk_size)
                downloaded_size += len(chunk)
                f.write(chunk)
    except:
        if os.path.exists(path):
             os.remove(path)
        raise RuntimeError('Stopped downloading due to interruption.')
    return path

if __name__ == '__main__':
    argparser = argparse.ArgumentParser("Partition graphs")
    argparser.add_argument('--num_parts', type=int, default=4,
                           help='number of partitions')
    argparser.add_argument('--part_method', type=str, default='metis',
                           help='the partition method')
    argparser.add_argument('--balance_train', action='store_true',
                           help='balance the training size in each partition.')
    argparser.add_argument('--undirected', action='store_true',
                           help='turn the graph into an undirected graph.')
    argparser.add_argument('--balance_edges', action='store_true',
                           help='balance the number of edges in each partition.')
    argparser.add_argument('--output', type=str, default='/etc/dgl/shared/data',
                           help='Output path of partitioned graph.')
    argparser.add_argument('--workspace', type=str, default='/dgl_workspace',
                           help='Path of user directory.')
    argparser.add_argument('--graph_name', type=str, default='ogb-product',
                           help='graph_name: ogb-product')
    argparser.add_argument('--rel_data_path', type=str, required=True,
                           help='Relative dataset path in workspace')
    argparser.add_argument('--dataset_url', type=str, required=True,
                           help='Dataset url')
    args = argparser.parse_args()
    args.output = f'{args.workspace}/{args.rel_data_path}'
    print(f'Partition arguments: {args}')
    
    start = time.time()
    g, _ = load_dataset('ogbn-products', args.output, args.dataset_url)
    print('load \'ogbn-products\' takes {:.3f} seconds'.format(time.time() - start))
    print('|V|={}, |E|={}'.format(g.number_of_nodes(), g.number_of_edges()))
    print('train: {}, valid: {}, test: {}'.format(th.sum(g.ndata['train_mask']),
                                                  th.sum(g.ndata['val_mask']),
                                                  th.sum(g.ndata['test_mask'])))
    if args.balance_train:
        balance_ntypes = g.ndata['train_mask']
    else:
        balance_ntypes = None

    if args.undirected:
        sym_g = dgl.to_bidirected(g, readonly=True)
        for key in g.ndata:
            sym_g.ndata[key] = g.ndata[key]
        g = sym_g

    dgl.distributed.partition_graph(g, args.graph_name, args.num_parts, args.output,
                                    part_method=args.part_method,
                                    balance_ntypes=balance_ntypes,
                                    balance_edges=args.balance_edges)