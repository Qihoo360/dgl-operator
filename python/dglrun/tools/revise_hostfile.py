import sys
import argparse
import signal
import logging
import time


def reviseIPConfigForDGLKE(workspace, ip_config, num_servers):
    local_ip_config = workspace + '/hostfile_revised'
    tuples = []
    with open(ip_config, "r") as f:
        for row in f:
            tuples.append(row)
    with open(local_ip_config, "w") as f:
        for el in tuples:
            li = el.split()
            f.write(f'{li[0]} {li[1]} {num_servers}\n')

def reviseIPConfigForDGL(workspace, ip_config):
    local_ip_config = workspace + '/hostfile_revised'
    tuples = []
    with open(ip_config, "r") as f:
        for row in f:
            tuples.append(row)
    with open(local_ip_config, "w") as f:
        for el in tuples:
            li = el.split()
            f.write(f'{li[0]} {li[1]}\n')

def main():
    parser = argparse.ArgumentParser(description='Revise hostfile')
    parser.add_argument('--workspace', type=str,
                        help='Path of user directory of distributed tasks. \
                        This is used to specify a destination location where \
                        the contents of current directory will be rsyncd')
    parser.add_argument('--ip_config', type=str,
                        help='The file (in workspace) of IP configuration for server processes')
    parser.add_argument('--num_servers', type=int,
                        help='The number of server processes per machine')
    parser.add_argument('--framework', type=str, required=True)
    args, _ = parser.parse_known_args()

    if args.framework == 'DGL':
        reviseIPConfigForDGL(args.workspace, args.ip_config)
    elif args.framework == 'DGLKE':
        reviseIPConfigForDGLKE(args.workspace, args.ip_config, args.num_servers)

def signal_handler(signal, frame):
    logging.info('Stop launcher')
    sys.exit(0)

if __name__ == '__main__':
    fmt = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(format=fmt, level=logging.INFO)
    signal.signal(signal.SIGINT, signal_handler)
    main()