import numpy as np
from math import isnan
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from functools import reduce

clients = [1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 95, 100]
virtuoso_clients = [1, 5, 10, 15, 20, 25, 30]


def compute_delta_time(base_path, clients):
    """
        Compute Delta_x = E_x - E_0, where:
        - E_O is the execution time with no concurrent clients
        - E_x is the execution time with x concurrent clients

        E_x = E_0 + alpha * n
        alpha * n = E_x - E_0
    """
    results = []
    base_times = dict()
    last = 0
    for nb_clients in clients:
        df = np.genfromtxt('{}/run1/execution_times_{}c.csv'.format(base_path, nb_clients),
                           delimiter=',', names=True, dtype=None, encoding='utf-8')
        if nb_clients == 1:
            for row in df:
                base_times[row['query']] = row['time']
            results.append(0)
        else:
            tmp = []
            for row in df:
                value = row['time'] - base_times[row['query']]
                if value <= 0:
                    tmp.append(0)
                else:
                    tmp.append(value)
            results.append(np.mean(tmp))
    return results


sage_75 = compute_delta_time('results/watdiv-sage-75ms', clients)
sage_150 = compute_delta_time('results/watdiv-sage-150ms', clients)
virtuoso = compute_delta_time('results/watdiv-virtuoso-load', virtuoso_clients)
virtu_quota = compute_delta_time('results/watdiv-virtuo-quota-load', clients)
tpf = compute_delta_time('results/watdiv-ref-load', clients)

ax = plt.axes()
ax.grid()
plt.rc('text', usetex=True)
plt.tick_params(axis='both', which='major', labelsize=15)
plt.tight_layout()
plt.plot(clients, sage_75, label='Sage-75', marker='P')
# plt.plot(clients, sage_150, label='Sage-150', marker='X')
plt.plot(virtuoso_clients, virtuoso, label='Virtuoso no quota', marker='o')
plt.plot(clients, virtu_quota, label='Virtuoso quota', marker='X')
plt.plot(clients, tpf, label='TPF', marker='s', color='r')
plt.legend()
plt.xlabel('Number of concurrent clients (n)', fontsize=17)
plt.ylabel('$\\alpha n = E_n - E_0$', fontsize=17)
plt.tight_layout()
plt.show()
