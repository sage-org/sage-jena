import matplotlib.pyplot as plt
from utils import DrawPlot, compute_total_time, compute_nb_http, compute_overhead, compute_tffr, compute_data_transfers, compute_throughput


def add_latency(http_calls):
    LATENCY = 0.050  # in seconds
    return http_calls * LATENCY


clients = [1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50]
quotas = [50, 60, 70, 75, 77, 80, 100, 150, 200]

# Average completion time
virtuo_inf = compute_total_time('results/watdiv-virtuoso', clients, 2, suffix="virtuoso")
sage_75ms = compute_total_time('results/watdiv-sage-75ms', clients, 3, add_latency=True)
# sage_1s = compute_total_time('results/watdiv-sage-1s', clients, 2, add_latency=True)
tpf = compute_total_time('results/watdiv-tpf', clients, 3, suffix="tpf")

with DrawPlot('total_time', plt, 'Number of clients', 'Avg. completion time per client (s)') as figure:
    figure.plot(clients, virtuo_inf, linestyle='-', marker='o', color='r', label='Virtuoso')
    figure.plot(clients, sage_75ms, linestyle='-', marker='D', color='b', label='SaGe-75')
    # figure.plot(clients, sage_1s, linestyle='-', marker='x', color='c', label='SaGe-1')
    figure.plot(clients, tpf, linestyle='-', marker='s', color='g', label='TPF')
    figure.legend(ncol=4, loc='upper center', bbox_to_anchor=(0.5, 1.2), fontsize=13, fancybox=False)


# virtuo_inf = compute_throughput('results/watdiv-virtuoso', clients, 2, suffix="virtuoso")
# sage_75ms = compute_throughput('results/watdiv-sage-75ms', clients, 3, add_latency=True)
# tpf = compute_throughput('results/watdiv-tpf', clients, 3, suffix="tpf")
#
# print(virtuo_inf[-1:])
# print(sage_75ms[-1:])
# print(tpf[-1:])
# with DrawPlot('throughput', plt, 'Number of clients', 'Avg. throughput (s)', yscale='linear') as figure:
#     figure.plot(clients, virtuo_inf, linestyle='-', marker='o', color='r', label='Virtuoso')
#     figure.plot(clients, sage_75ms, linestyle='-', marker='D', color='b', label='SaGe-75')
#     figure.plot(clients, tpf, linestyle='-', marker='s', color='g', label='TPF')
#     figure.legend(ncol=4, loc='upper center', bbox_to_anchor=(0.5, 1.2), fontsize=13, fancybox=False)

# Time for first results
virtuo_inf = compute_tffr('results/watdiv-virtuoso', clients, 2, suffix="virtuoso")
sage_75ms = compute_tffr('results/watdiv-sage-75ms', clients, 3)

with DrawPlot('time_first_results', plt, 'Number of clients', 'Avg. time for First results (s)', yscale='linear') as figure:
    figure.plot(clients, virtuo_inf, linestyle='-', marker='o', color='r', label='Virtuoso')
    figure.plot(clients, sage_75ms, linestyle='-', marker='D', color='b', label='SaGe-75')
    figure.legend(ncol=4, loc='upper center', bbox_to_anchor=(0.5, 1.2), fontsize=13, fancybox=False)


# Number of HTTP requests
virtuo_inf = compute_nb_http('results/watdiv-virtuoso', [50], 2, suffix="virtuoso")
sage_75ms = compute_nb_http('results/watdiv-sage-75ms', [50], 3)
tpf = compute_nb_http('results/watdiv-tpf', [50], 3, suffix="tpf")

with DrawPlot('http_requests', plt, 'Approach', 'Avg. number of HTTP requests') as figure:
    figure.bar(['Virtuoso', 'Sage-75', 'TPF'], virtuo_inf + sage_75ms + tpf, color=['r', 'b', 'g'], width=[0.6, 0.6, 0.6])

# Data transfers
# virtuo_inf = compute_nb_http('results/watdiv-virtuoso', [50], 2, suffix="virtuoso")
sage_75ms = compute_data_transfers('results/watdiv-sage-75ms', clients, 3)
tpf = compute_data_transfers('results/watdiv-tpf', clients, 3, suffix="tpf")

with DrawPlot('data_transfers', plt, 'Number of clients', 'Avg. data transferred (mo)') as figure:
    figure.plot(clients, sage_75ms, linestyle='-', marker='D', color='b', label='SaGe-75')
    figure.plot(clients, tpf, linestyle='-', marker='s', color='g', label='TPF')
    figure.legend(ncol=4, loc='upper center', bbox_to_anchor=(0.5, 1.2), fontsize=13, fancybox=False)

# overhead
sage_75ms_import = compute_overhead('results/watdiv-sage-75ms', clients, 3)
sage_75ms_export = compute_overhead('results/watdiv-sage-75ms', clients, 3, metric="exportTime")

with DrawPlot('overhead', plt, 'Number of clients', 'Avg. preemption overhead (ms)', yscale='linear') as figure:
    figure.plot(clients, sage_75ms_import, linestyle='-', marker='s', color='b', label='\\texttt{Resume}')
    figure.plot(clients, sage_75ms_export, linestyle='-', marker='D', color='r', label='\\texttt{Suspend}')
    figure.legend(ncol=4, loc='upper center', bbox_to_anchor=(0.5, 1.2), fontsize=13, fancybox=False)
