# bgp_interface.py
# Author: Thomas MINIER - MIT License 2017-2018
from argparse import ArgumentParser
from multiprocessing import Process, Event
from subprocess import call, Popen


def run_reference(index, barriers, queries, bench_query, output, nb_clients, reference):
    if index == nb_clients - 1:
        command = ['bash', 'scripts/run_reference.sh', queries, output]
        barriers[0].wait()
        call(command)
    else:
        command = ['bash', 'scripts/run_reference_load.sh', queries]
        barriers[0].wait()
        p = Popen(command)
        barriers[1].wait()
        p.terminate()


def run_virtuoso(index, barriers, queries, bench_query, output, nb_clients, reference):
    if index == nb_clients - 1:
        command = ['bash', 'scripts/run_virtuoso.sh', queries, output]
        barriers[0].wait()
        call(command)
    else:
        command = ['bash', 'scripts/run_virtuoso_load.sh', queries]
        barriers[0].wait()
        p = Popen(command)
        barriers[1].wait()
        p.terminate()


def run_brtpf(index, barriers, queries, bench_query, output, nb_clients, reference):
    if index == nb_clients - 1:
        command = ['bash', 'scripts/run_brtpf.sh', queries, output, reference]
        barriers[0].wait()
        call(command)
    else:
        command = ['bash', 'scripts/run_brtpf_load.sh', queries]
        barriers[0].wait()
        p = Popen(command)
        barriers[1].wait()
        p.terminate()


def run_bgp(index, barriers, queries, bench_query, output, nb_clients, reference):
    if index == nb_clients - 1:
        command = ['bash', 'scripts/run_bgp.sh', queries, output, reference]
        barriers[0].wait()
        call(command)
    else:
        command = ['bash', 'scripts/run_bgp_load.sh', queries]
        barriers[0].wait()
        p = Popen(command)
        barriers[1].wait()
        p.terminate()


if __name__ == '__main__':
    parser = ArgumentParser(description='Run the load experiment')
    parser.add_argument('queries', metavar='queries', help='Directory where benchmark queries are stored')
    parser.add_argument('bench', metavar='benchquery', help='Query used to generate load')
    parser.add_argument('output', metavar='output', help='Output for the measurements')
    parser.add_argument('reference', metavar='reference', help='Directory where reference results are stored')
    parser.add_argument('clients', metavar='clients', type=int, help='Number of clients')
    parser.add_argument('type', metavar='type', help='Type of experiment')

    args = parser.parse_args()
    barriers = [Event(), Event()]
    processes = []
    ref_process = None
    function = None
    if args.type == 'virtuoso':
        function = run_virtuoso
    elif args.type == 'reference' or args.type == 'ref':
        function = run_reference
    elif args.type == 'brtpf':
        function = run_brtpf
    elif args.type == 'bgp':
        function = run_bgp
    else:
        raise Exception('Unknown experiment type (either reference, brtpf or bgp)')
    for i in range(args.clients):
        p = Process(target=function, args=(i, barriers, args.queries, args.bench, args.output, args.clients, args.reference))
        p.start()
        processes.append(p)
        ref_process = p
    barriers[0].set()
    ref_process.join()
    barriers[1].set()
    for p in processes:
        p.join()
