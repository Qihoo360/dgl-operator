"""Copy the partitions to a cluster of machines."""
import os
import stat
import sys
import subprocess
import argparse
import signal
import logging
import json
import copy


def kubecp_cmd(source_file_name, pod_name, path):
    print('copy {} to {}'.format(source_file_name, pod_name + ':' + path))
    cmd = f'set -x; /opt/kube/kubectl cp {source_file_name} {pod_name}:{path}'
    subprocess.check_call(cmd, shell = True)

def kubexec_cmd(pod_name, cmd):
    cmd = f'sh /etc/dgl/kubexec.sh {pod_name} \'{cmd}\''
    subprocess.check_call(cmd, shell = True)

def make_local_dir(path):
    cmd = f'mkdir -p {path}'
    subprocess.check_call(cmd, shell = True)

def main():
    parser = argparse.ArgumentParser(description='Copy data to the servers.')
    parser.add_argument('--workspace', type=str, required=True,
                        help='Path of user directory of distributed tasks. \
                        This is used to specify a destination location where \
                        data are copied to on remote machines.')
    parser.add_argument('--rel_data_path', type=str, required=True,
                        help='Relative path in workspace to store the partition data.')
    parser.add_argument('--rel_workload_path', type=str, required=True,
                        help='Relative path in workspace to store the workload data.')
    parser.add_argument('--part_config', type=str, required=True,
                        help='The partition config file. The path is on the local machine (worker chief).')
    parser.add_argument('--ip_config', type=str, required=True,
                        help='The file of IP configuration for servers. \
                        The path is on the local machine.')
    args = parser.parse_args()

    hosts = []
    with open(args.ip_config) as f:
        for line in f:
            res = line.strip().split(' ')
            pod_name = res[2]
            hosts.append(pod_name)
    
    # We need to update the partition config file so that the paths are relative to
    # the workspace in the remote machines.
    with open(args.part_config) as conf_f:
        part_metadata = json.load(conf_f)
        worker_part_metadata = copy.deepcopy(part_metadata)
        worker_chief_part_metadata = copy.deepcopy(part_metadata)
        num_parts = worker_chief_part_metadata['num_parts']
        graph_name = worker_chief_part_metadata['graph_name']
        assert num_parts == len(hosts), \
                'The number of partitions needs to be the same as the number of hosts.'

        # update worker workload path
        for part_id in range(num_parts):
            part_files = worker_part_metadata['part-{}'.format(part_id)]
            part_files['edge_feats'] = '{}/{}/part{}/edge_feat.dgl'.format(args.workspace, args.rel_workload_path, part_id)
            part_files['node_feats'] = '{}/{}/part{}/node_feat.dgl'.format(args.workspace, args.rel_workload_path, part_id)
            part_files['part_graph'] = '{}/{}/part{}/graph.dgl'.format(args.workspace, args.rel_workload_path, part_id)

            part_files = worker_chief_part_metadata['part-{}'.format(part_id)]
            part_files['edge_feats'] = '{}/{}/part{}/edge_feat.dgl'.format(args.workspace, args.rel_data_path, part_id)
            part_files['node_feats'] = '{}/{}/part{}/node_feat.dgl'.format(args.workspace, args.rel_data_path, part_id)
            part_files['part_graph'] = '{}/{}/part{}/graph.dgl'.format(args.workspace, args.rel_data_path, part_id)
    
    local_workload_dir = '{}/{}'.format(args.workspace, args.rel_workload_path)
    make_local_dir(local_workload_dir)
    worker_part_config = '{}/{}.json'.format(local_workload_dir, graph_name)
    with open(worker_part_config, 'w') as outfile:
        json.dump(worker_part_metadata, outfile, sort_keys=True, indent=4)

    # Copy files
    for part_id, pod_name in enumerate(hosts):
        remote_path = '{}/{}'.format(args.workspace, args.rel_workload_path)
        kubexec_cmd(pod_name, 'mkdir -p {}'.format(remote_path))
        # copy updated part config json
        kubecp_cmd(worker_part_config, pod_name, remote_path)
        # copy part files
        remote_part_path = '{}/part{}'.format(remote_path, part_id)
        kubexec_cmd(pod_name, 'mkdir -p {}'.format(remote_part_path))
        worker_chief_part_files = worker_chief_part_metadata['part-{}'.format(part_id)]
        kubecp_cmd(worker_chief_part_files['node_feats'], pod_name, remote_part_path)
        kubecp_cmd(worker_chief_part_files['edge_feats'], pod_name, remote_part_path)
        kubecp_cmd(worker_chief_part_files['part_graph'], pod_name, remote_part_path)


def signal_handler(signal, frame):
    logging.info('Stop copying')
    sys.exit(0)

if __name__ == '__main__':
    fmt = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(format=fmt, level=logging.INFO)
    signal.signal(signal.SIGINT, signal_handler)
    main()