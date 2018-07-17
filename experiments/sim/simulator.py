# Sage simulator
# author: Thomas Minier
import numpy as np
from random import randrange, uniform
from argparse import ArgumentParser
from multiprocessing import Pool
import csv

parser = ArgumentParser(description='Run the Sage simulator')
parser.add_argument('queries', metavar='queries', help='Queries config file (in csv format)')
parser.add_argument('quota', metavar='quota', help='Server time quota (in ms)', type=int)
parser.add_argument('nb_clients', metavar='clients', help='Number of concurrent clients', type=int)
parser.add_argument('nb_runs', metavar='runs', help='Number of runs (to compute an average value)', type=int)
parser.add_argument('-o, --output', dest="output", help='Output file (csv)')
# parser.add_argument('-w, --workers', dest="workers", default=4, type=int, help='sum the integers (default: find the max)')

args = parser.parse_args()

nb_runs = args.nb_runs
queries_file = args.queries
nb_clients = args.nb_clients
quota = args.quota / 1000  # ms
output_file = args.output if args.output is not None else "./simulation_{}c_{}q_{}runs.csv".format(nb_clients, args.quota, nb_runs)

# Load queries
with open(queries_file, newline='') as csvfile:
    queries_reader = csv.DictReader(csvfile, delimiter=',')
    queries = [row for row in queries_reader]


def compute_run(exec_time, quota, http_calls, nb_clients):
    def compute_latency(base, delta=10):
        return uniform(base - delta, base + delta)

    res = 0
    # compute execution time for each http call
    for i in range(0, http_calls, 1):
        queue_pos = randrange(0, nb_clients, 1) if nb_clients > 1 else 0
        res += exec_time + (queue_pos * quota)
    return res


output_queries = np.array([])

with Pool(processes=4) as pool:
    for query in queries:
        q_name = query['query']
        http_calls = int(query['httpCalls'])
        exec_time = float(query['time']) / http_calls
        times = np.array([])
        # for each run
        times = np.append(times, [pool.apply_async(compute_run, (exec_time, quota, http_calls, nb_clients,)) for r in range(0, nb_runs, 1)])
        # unwrap times
        final_time = np.mean(list(map(lambda x: x.get(), times)))
        output_queries = np.append(output_queries, [{'query': q_name, 'httpCalls': http_calls, 'time': final_time}])
        # print('query {}, exec time = {} s'.format(q_name, final_time))

# write output
with open(output_file, 'w+', newline='') as csvfile:
    fieldnames = ['query', 'time', 'httpCalls']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for out_query in output_queries:
        writer.writerow(out_query)
