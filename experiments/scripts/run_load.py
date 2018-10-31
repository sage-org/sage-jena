# bgp_interface.py
# Author: Thomas MINIER - MIT License 2017-2018
from argparse import ArgumentParser
from multiprocessing import Process, Event
from subprocess import call
from os import listdir
import time


def run_tpf(i, barrier, queries, output):
    command = ['bash', 'scripts/run_tpf.sh', queries, output]
    barrier.wait()
    call(command)


def run_virtuoso(i, barrier, queries, output):
    command = ['bash', 'scripts/run_virtuoso.sh', queries, output]
    barrier.wait()
    call(command)


def run_brtpf(i, barrier, queries, output):
    command = ['bash', 'scripts/run_brtpf.sh', queries, output]
    barrier.wait()
    call(command)


def run_sage(i, barrier, queries, output):
    command = ['bash', 'scripts/run_bgp.sh', queries, output]
    barrier.wait()
    call(command)


if __name__ == '__main__':
    parser = ArgumentParser(description='Run the load experiment')
    parser.add_argument('queries', metavar='queries', help='Directory where benchmark query mixes are stored')
    parser.add_argument('output', metavar='output', help='Root output for measurements')
    parser.add_argument('clients', metavar='clients', type=int, help='Number of clients')
    parser.add_argument('type', metavar='type', help='Type of experiment')

    args = parser.parse_args()
    barrier = Event()
    processes = []
    function = None
    # select task runner based on the selected approach
    if args.type == 'virtuoso':
        function = run_virtuoso
    elif args.type == 'tpf':
        function = run_tpf
    elif args.type == 'brtpf':
        function = run_brtpf
    elif args.type == 'sage':
        function = run_sage
    else:
        raise Exception('Unknown experiment type (either tpf, brtpf, sage or virtuoso)')
    query_mixes = listdir(args.queries)
    query_mixes.sort()
    # start all clients, which will wait on a rendez-vous point before executing
    barrier.clear()
    for i in range(args.clients):
        query_mix = query_mixes[i % len(query_mixes)]
        mix_directory = "{}/{}".format(args.queries, query_mix)
        measure_output = "{}/{}clients/mix_{}/".format(args.output, args.clients, query_mix)
        # start process
        p = Process(target=function, args=(i, barrier, mix_directory, measure_output))
        p.start()
        processes.append(p)
    # trigger rendez-vous point and then wait for all clients to have been executed
    time.sleep(3)
    barrier.set()
    for p in processes:
        p.join()
