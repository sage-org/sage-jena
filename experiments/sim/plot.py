import numpy as np
from math import isnan
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from functools import reduce


def compute_avg_value(fn):
    def process(basePath, clients, nbRuns):
        res = []
        for nb in clients:
            values = []
            for x in range(1, nbRuns + 1):
                df = np.genfromtxt('{}/run{}/execution_times_{}c.csv'.format(basePath, x, nb), delimiter=',',
                                   names=True, dtype=None, encoding='utf-8')
                values += [fn(df)]
            res += [np.mean(values)]
        return res
    return process


def compute_avg_value2(basePath, quota, nbClients, fn):
    res = []
    for nb in nbClients:
        df = np.genfromtxt('{}/simulation_{}c_{}q_30runs.csv'.format(basePath, nb, quota), delimiter=',',
                           names=True, dtype=None, encoding='utf-8')
        res += [fn(df)]
    return res


def compute_throughput(basePath, clients, nbRuns=3):
    def mapper(x):
        # if x >= 120.0:
        #     return 120.0
        return x

    def processor(df):
        times = list(map(mapper, df['time']))
        return (len(df) * 3600) / np.sum(times)
    return compute_avg_value(processor)(basePath, clients, nbRuns)


def compute_throughput2(basePath, quota, clients, nbRuns=3):
    def mapper(x):
        # if x >= 120.0:
        #     return 120.0
        return x

    def processor(df):
        times = list(map(mapper, df['time']))
        return (len(df) * 3600) / np.sum(times)
    return compute_avg_value2(basePath, quota, clients, processor)


clients = [1, 5, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]

# Query throughput
vnq = compute_throughput('../results/watdiv-virtuoso-load', clients)
sim_vnq = compute_throughput2('./virtuo-nq', 120000, clients)
sage_75ms = compute_throughput('../results/watdiv-sage-75ms', clients)
sim_75ms = compute_throughput2('./sage-75ms', 75, clients)

plt.rcParams["figure.figsize"] = [5, 5]
plt.rcParams["legend.columnspacing"] = 1.0
fig = plt.figure()
plt.rc('text', usetex=True)
ax = plt.axes(yscale='log')
ax.grid()
plt.plot(clients, vnq, linestyle='-', marker='X', color='b', label='VNQ')
plt.plot(clients, sim_vnq, linestyle='-', marker='o', color='g', label='VNQ simul')
plt.plot(clients, sage_75ms, linestyle='-', marker='X', color='c', label='SaGe-75')
plt.plot(clients, sim_75ms, linestyle='-', marker='o', color='r', label='SaGe-75 simul')
plt.legend()
plt.xlabel('Number of clients', fontsize=17)
plt.ylabel('Query throughput (q/hr)', fontsize=17)
plt.tick_params(axis='both', which='major', labelsize=15)
plt.tight_layout()
plt.show()
