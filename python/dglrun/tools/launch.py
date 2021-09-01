"""Launching tool for DGL distributed training"""
import os
import stat
import sys
import subprocess
import argparse
import signal
import logging
import time
import json
from threading import Thread


def kubexec_multi(cmd, host, thread_list):
    cmd = f'sh /etc/dgl/kubexec.sh {host} \'{cmd}\''
    # thread func to run the job
    def run(cmd):
        subprocess.check_call(cmd, shell = True)

    thread = Thread(target = run, args=(cmd,))
    thread.setDaemon(True)
    thread.start()
    thread_list.append(thread)

def kubexec(cmd, host):
    cmd = f'sh /etc/dgl/kubexec.sh {host} \'{cmd}\''
    subprocess.check_call(cmd, shell = True)

def kubexec_container(cmd, host, container):
    cmd = f'sh /etc/dgl/kubexec.sh \'{host} -c {container}\' \'{cmd}\''
    subprocess.check_call(cmd, shell = True)

def exec(cmd):
    cmd = f'sh \'{cmd}\''
    subprocess.check_call(cmd, shell = True)

def kubecp(source_path, pod_name, target_dir):
    print('copy {} to {}'.format(source_path, pod_name + ':' + target_dir))
    cmd = f'set -x; /opt/kube/kubectl cp {source_path} {pod_name}:{target_dir}'
    subprocess.check_call(cmd, shell = True)

def kubecp_container(source_path, pod_name, target_dir, container):
    print('copy {} to {}'.format(source_path, pod_name + ':' + target_dir))
    cmd = f'set -x; /opt/kube/kubectl cp {source_path} {pod_name}:{target_dir} -c {container}'
    subprocess.check_call(cmd, shell = True)

def cp(source_path, target_dir):
    print('local copy {} to {}'.format(source_path, target_dir))
    cmd = f'cp {source_path} {target_dir}'
    subprocess.check_call(cmd, shell = True)

def get_ip_host_pairs(ip_config):
    hosts = []
    with open(ip_config) as f:
        for line in f:
            result = line.strip().split()
            if len(result) >= 3:
                ip = result[0]
                host = result[2]
                hosts.append((ip, host))
            else:
                raise RuntimeError("Format error of ip_config.")
    return hosts

def run_exec(args, udf_command):
    for pod_info in get_ip_host_pairs(args.ip_config):
        pod_name = pod_info[1]
        kubexec(udf_command, pod_name)

def run_cp(args, handle_rw=False):
    for pod_info in get_ip_host_pairs(args.ip_config):
        pod_name = pod_info[1]
        for source_file_path in args.source_file_paths.split():
            if handle_rw:
                cp(source_file_path, args.workspace)
                kubexec(f'mkdir -p {args.target_dir}', pod_name)
                kubecp(f'{args.workspace}/{source_file_path.split("/")[-1]}', pod_name, args.target_dir)
            else:
                kubexec(f'mkdir -p {args.target_dir}', pod_name)
                kubecp(source_file_path, pod_name, args.target_dir)

def run_cp_container(args):
    for pod_info in get_ip_host_pairs(args.ip_config):
        pod_name = pod_info[1]
        for source_file_path in args.source_file_paths.split():
            kubexec_container(f'mkdir -p {args.target_dir}', pod_name, args.container)
            kubecp_container(source_file_path, pod_name, args.target_dir, args.container)

def submit_jobs(args, udf_command):
    """Submit distributed jobs (server and client processes) via ssh"""
    hosts = []
    thread_list = []
    server_count_per_machine = 0

    # Get the host addresses of the cluster.
    ip_config = args.ip_config
    with open(ip_config) as f:
        for line in f:
            result = line.strip().split()
            if len(result) >= 3:
                ip = result[0]
                host = result[2]
                hosts.append((ip, host))
            else:
                raise RuntimeError("Format error of ip_config.")
            server_count_per_machine = args.num_servers
    assert args.num_parts == len(hosts), \
            'The number of graph partitions has to match the number of machines in the cluster.'

    tot_num_clients = args.num_trainers * (1 + args.num_samplers) * len(hosts)
    # launch server tasks
    server_cmd = 'DGL_ROLE=server DGL_NUM_SAMPLER=' + str(args.num_samplers)
    server_cmd = server_cmd + ' ' + 'OMP_NUM_THREADS=' + str(args.num_server_threads)
    server_cmd = server_cmd + ' ' + 'DGL_NUM_CLIENT=' + str(tot_num_clients)
    server_cmd = server_cmd + ' ' + 'DGL_CONF_PATH=' + str(args.part_config)
    server_cmd = server_cmd + ' ' + 'DGL_IP_CONFIG=' + str(args.ip_config)
    server_cmd = server_cmd + ' ' + 'DGL_NUM_SERVER=' + str(args.num_servers)
    for i in range(len(hosts)*server_count_per_machine):
        _, pod_name = hosts[int(i / server_count_per_machine)]
        cmd = server_cmd + ' ' + 'DGL_SERVER_ID=' + str(i)
        cmd = cmd + ' ' + udf_command
        cmd = 'cd ' + str(args.workspace) + '; ' + cmd
        kubexec_multi(cmd, pod_name, thread_list)
    # launch client tasks
    client_cmd = 'DGL_DIST_MODE="distributed" DGL_ROLE=client DGL_NUM_SAMPLER=' + str(args.num_samplers)
    client_cmd = client_cmd + ' ' + 'DGL_NUM_CLIENT=' + str(tot_num_clients)
    client_cmd = client_cmd + ' ' + 'DGL_CONF_PATH=' + str(args.part_config)
    client_cmd = client_cmd + ' ' + 'DGL_IP_CONFIG=' + str(args.ip_config)
    client_cmd = client_cmd + ' ' + 'DGL_NUM_SERVER=' + str(args.num_servers)
    if os.environ.get('OMP_NUM_THREADS') is not None:
        client_cmd = client_cmd + ' ' + 'OMP_NUM_THREADS=' + os.environ.get('OMP_NUM_THREADS')
    if os.environ.get('PYTHONPATH') is not None:
        client_cmd = client_cmd + ' ' + 'PYTHONPATH=' + os.environ.get('PYTHONPATH')

    torch_cmd = '-m torch.distributed.launch'
    torch_cmd = torch_cmd + ' ' + '--nproc_per_node=' + str(args.num_trainers)
    torch_cmd = torch_cmd + ' ' + '--nnodes=' + str(len(hosts))
    torch_cmd = torch_cmd + ' ' + '--node_rank=' + str(0)
    torch_cmd = torch_cmd + ' ' + '--master_addr=' + str(hosts[0][0])
    torch_cmd = torch_cmd + ' ' + '--master_port=' + str(1234)
    for node_id, tu in enumerate(hosts):
        _, pod_name = tu
        new_torch_cmd = torch_cmd.replace('node_rank=0', 'node_rank='+str(node_id))
        if 'python3' in udf_command:
            new_udf_command = udf_command.replace('python3', 'python3 ' + new_torch_cmd)
        elif 'python2' in udf_command:
            new_udf_command = udf_command.replace('python2', 'python2 ' + new_torch_cmd)
        else:
            new_udf_command = udf_command.replace('python', 'python ' + new_torch_cmd)
        cmd = client_cmd + ' ' + new_udf_command
        cmd = 'cd ' + str(args.workspace) + '; ' + cmd
        kubexec_multi(cmd, pod_name, thread_list)

    for thread in thread_list:
        thread.join()

def main():
    parser = argparse.ArgumentParser(description='Launch a distributed job')
    parser.add_argument('--workspace', type=str,
                        help='Path of user directory of distributed tasks. \
                        This is used to specify a destination location where \
                        the contents of current directory will be rsyncd')
    parser.add_argument('--num_trainers', type=int,
                        help='The number of trainer processes per machine')
    parser.add_argument('--num_samplers', type=int, default=0,
                        help='The number of sampler processes per trainer process')
    parser.add_argument('--num_servers', type=int,
                        help='The number of server processes per machine')
    parser.add_argument('--num_parts', type=int,
                        help='The number of partitions')
    parser.add_argument('--part_config', type=str,
                        help='The file of the partition config on worker node')
    parser.add_argument('--ip_config', type=str,
                        help='The file (in workspace) of IP configuration for server processes')
    parser.add_argument('--num_server_threads', type=int, default=1,
                        help='The number of OMP threads in the server process. \
                        It should be small if server processes and trainer processes run on \
                        the same machine. By default, it is 1.')
    parser.add_argument('--target_dir', type=str, default="/dgl_workspace",
                        help='Path of user directory.')
    parser.add_argument('--cmd_type', type=str)
    parser.add_argument('--source_file_paths', type=str)
    parser.add_argument('--container', type=str)
    args, udf_command = parser.parse_known_args()
    print(f'Launch arguments: {args}, {udf_command}')

    assert args.cmd_type is not None, 'A user has to specify --cmd_type.'
    assert args.ip_config is not None, \
        'A user has to specify an IP configuration file with --ip_config.'
    if args.cmd_type == 'exec_batch':
        assert len(udf_command) == 1, 'Please provide user command line.'
        udf_command = str(udf_command[0])
        if 'python' not in udf_command and \
            'dglke_partition' not in udf_command and \
            'dglke_dist_train' not in udf_command and \
            'dglke_train' not in udf_command:
            raise RuntimeError("Only support Python executable, dglke_partition, dglke_train and dglke_dist_train.")
        run_exec(args, udf_command)
    elif args.cmd_type == 'copy_batch':
        assert args.workspace is not None, 'A user has to specify a workspace with --workspace.'
        assert args.target_dir is not None, \
            'A user has to specify a target_dir with --target_dir.'
        assert args.source_file_paths is not None, \
            'A user has to specify a source file path with --source_file_paths.'
        run_cp(args)
    elif args.cmd_type == 'copy_batch_container':
        assert args.workspace is not None, 'A user has to specify a workspace with --workspace.'
        assert args.container is not None, 'A user has to specify a container with --container.'
        assert args.target_dir is not None, \
            'A user has to specify a target_dir with --target_dir.'
        assert args.source_file_paths is not None, \
            'A user has to specify a source file path with --source_file_paths.'
        run_cp_container(args)
    elif args.cmd_type == 'train':
        assert len(udf_command) == 1, 'Please provide user command line.'
        assert args.num_trainers is not None and args.num_trainers > 0, \
                '--num_trainers must be a positive number.'
        assert args.num_samplers is not None and args.num_samplers >= 0, \
                '--num_samplers must be a non-negative number.'
        assert args.num_servers is not None and args.num_servers > 0, \
                '--num_servers must be a positive number.'
        assert args.num_server_threads > 0, '--num_server_threads must be a positive number.'
        assert args.workspace is not None, 'A user has to specify a workspace with --workspace.'
        assert args.part_config is not None, \
                'A user has to specify a partition configuration file with --part_config.'
        assert args.ip_config is not None, \
                'A user has to specify an IP configuration file with --ip_config.'
        udf_command = str(udf_command[0])
        if 'python' not in udf_command:
            raise RuntimeError("DGL launching script can only support Python executable file.")
        submit_jobs(args, udf_command)

def signal_handler(signal, frame):
    logging.info('Stop launcher')
    sys.exit(0)

if __name__ == '__main__':
    fmt = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(format=fmt, level=logging.INFO)
    signal.signal(signal.SIGINT, signal_handler)
    main()