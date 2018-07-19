import numpy as np
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages


def add_latency(http_calls):
    LATENCY = 0.050  # in seconds
    return http_calls * LATENCY


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


def compute_avg_value2(basePath, quota, nbClients, nbRuns, fn):
    res = []
    for q in quota:
        values = []
        for x in range(1, nbRuns + 1):
            df = np.genfromtxt('{}/run{}/execution_times_{}ms_{}c.csv'.format(basePath, x, q, nbClients), delimiter=',',
                               names=True, dtype=None, encoding='utf-8')
            values += [fn(df)]
        res += [np.mean(values)]
    return res


def compute_throughput(basePath, clients, nbRuns=3):
    def mapper(df):
        t = df['time']
        if t >= 120.0:
            return 120.0
        # manually add network latency
        return t + add_latency(df['httpCalls'])

    def processor(df):
        times = list(map(mapper, df))
        return (len(df) * 3600) / np.sum(times)
    return compute_avg_value(processor)(basePath, clients, nbRuns)


def compute_throughput_quota(basePath, quotas, nbClients, nbRuns=3):
    def processor(df):
        return (len(df) * 3600) / np.sum(df['time'])
    return compute_avg_value2(basePath, quotas, nbClients, nbRuns, processor)


def compute_resp_time_quota(basePath, quotas, nbClients, nbRuns=3):
    def processor(df):
        return (np.sum(df['httpCalls']) * 3600000) / np.sum(df['serverTime'])
    return compute_avg_value2(basePath, quotas, nbClients, nbRuns, processor)


def compute_nb_http_quota(basePath, quotas, nbClients, nbRuns=3):
    def processor(df):
        return np.sum(df['httpCalls'])
    return compute_avg_value2(basePath, quotas, nbClients, nbRuns, processor)


def compute_nb_http(basePath, clients, nbRuns=3):
    def processor(df):
        return np.sum(df['httpCalls'])
    return compute_avg_value(processor)(basePath, clients, nbRuns)


def compute_overhead(basePath, clients, nbRuns=3):
    def processor(df):
        return np.mean(df['overhead'])
    return compute_avg_value(processor)(basePath, clients, nbRuns)


def compute_timeouts(basePath, clients, nbRuns=3):
    def processor(df):
        nb = 0
        for row in df:
            if row['time'] >= 120.0 or row['errors'] > 0:
                nb += 1
        return (nb / len(df)) * 100
    return compute_avg_value(processor)(basePath, clients, nbRuns)


def compute_virtuo_timeouts(basePath, clients):
    def processor(df):
        nb = 0
        for row in df:
            if row['time'] >= 120.0 or row['nbResults'] >= 2000:
                nb += 1
        return (nb / len(df)) * 100
    return compute_avg_value(processor)(basePath, clients, 3)


def compute_completeness(basePath, clients, nbRuns=3):
    def processor(df):
        values = list(map(lambda x: x * 100, df['completeness']))
        return np.mean(values)
    return compute_avg_value(processor)(basePath, clients, nbRuns)


def compute_virtuo_completeness(refPath, basePath, clients, nbRuns=3):
    def cntResults(df):
        return np.sum(df['nbResults'])
    refNbResults = compute_avg_value(cntResults)(refPath, [1], 1)[0]
    avgNbresults = compute_avg_value(cntResults)(basePath, clients, nbRuns)
    return list(map(lambda x: (x / refNbResults) * 100, avgNbresults))


def compute_total_exec_time(basePath, clients, nbRuns=3):
    def processor(df):
        return np.sum(df['time'])
    return compute_avg_value(processor)(basePath, clients, nbRuns)


clients = [1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 95, 100]
quotas = [50, 60, 70, 75, 77, 80, 100, 150, 200]

# Query throughput per quota
quota_25c = compute_throughput_quota('results/quotas', quotas, 25, 1)

plt.rcParams["figure.figsize"] = [5, 4]
fig = plt.figure()
plt.rc('text', usetex=True)
ax = plt.axes()
ax.grid()
plt.plot(quotas, quota_25c, linestyle='-', marker='o', color='b', label='25 clients')
# plt.legend(title='', fontsize=15)
plt.xlabel('Quota (ms)', fontsize=18)
plt.ylabel('Query throughput (q/hr)', fontsize=18)
plt.tick_params(axis='both', which='major', labelsize=15)
plt.tight_layout()
with PdfPages('scripts/plots/quota.pdf') as pdf:
    pdf.savefig(fig)
    print('generated quota plot at scripts/plots/quotas.pdf')

plt.clf()

# total execution time
virtuo_quota = compute_total_exec_time('results/watdiv-virtuo-quota-load', clients)
virtuo_pages = compute_total_exec_time('results/watdiv-virtuo-pages-load', clients)
virtuo_inf = compute_total_exec_time('results/watdiv-virtuoso-load', clients)
sage_150ms = compute_total_exec_time('results/watdiv-sage-150ms', clients)
sage_75ms = compute_total_exec_time('results/watdiv-sage-75ms', clients)
brtpf = compute_total_exec_time('results/watdiv-brtpf-load', [1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 75, 100])
tpf = compute_total_exec_time('results/watdiv-ref-load', clients, 1)

plt.rcParams["figure.figsize"] = [5, 5]
plt.rcParams["legend.columnspacing"] = 1.0
fig = plt.figure()
plt.rc('text', usetex=True)
ax = plt.axes(yscale='log')
ax.grid()
plt.plot(clients, virtuo_quota, linestyle='--', marker='o', color='b', label='VQ')
plt.plot(clients, virtuo_pages, linestyle='--', marker='.', color='m', label='VQP')
plt.plot(clients, virtuo_inf, linestyle='-', marker='o', color='r', label='VNQ')
plt.plot(clients, sage_150ms, linestyle='-', marker='X', color='y', label='SaGe-150')
plt.plot(clients, sage_75ms, linestyle='-', marker='X', color='c', label='SaGe-75')
plt.plot([1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 75, 100], brtpf, linestyle='-', marker='D', color='k', label='BrTPF')
plt.plot(clients, tpf, linestyle='-', marker='s', color='g', label='TPF')
plt.legend(ncol=4, loc='upper center', bbox_to_anchor=(0.5, 1.2), fontsize=13, fancybox=False)
plt.xlabel('Number of clients', fontsize=17)
plt.ylabel('Total execution time (s)', fontsize=17)
plt.tick_params(axis='both', which='major', labelsize=15)
plt.tight_layout()
plt.savefig('scripts/plots/total_time.png', format='png', dpi=1000)
with PdfPages('scripts/plots/total_time.pdf') as pdf:
    pdf.savefig(fig)
    print('generated total execution time plot at scripts/plots/total_time.pdf')

plt.clf()

# Total execution time 4 workers
virtuo_quota_4w = compute_total_exec_time('results/watdiv-virtuoso4w-load', clients)
virtuo_quota_1w = compute_total_exec_time('results/watdiv-virtuoso-load', clients)
sage_75ms_4w = compute_total_exec_time('results/watdiv-sage-4w', clients)
sage_75ms_1w = compute_total_exec_time('results/watdiv-sage-150ms', clients)
plt.rcParams["figure.figsize"] = [5, 3]
plt.rcParams["legend.columnspacing"] = 0.8
fig = plt.figure()
plt.rc('text', usetex=True)
ax = plt.axes(yscale='log')
ax.grid()
plt.plot(clients, virtuo_quota_4w, linestyle='-', marker='P', color='r', label='VNQ 4w')
plt.plot(clients, virtuo_quota_1w, linestyle='--', marker='x', color='r', label='VNQ 1w')
plt.plot(clients, sage_75ms_4w, linestyle='-', marker='P', color='c', label='SaGe-75 4w')
plt.plot(clients, sage_75ms_1w, linestyle='--', marker='x', color='c', label='SaGe-75 1w')
plt.legend(ncol=2, fontsize=13, fancybox=False)
plt.xlabel('Number of clients', fontsize=17)
plt.ylabel('Total execution time (s)', fontsize=17)
plt.tick_params(axis='both', which='major', labelsize=15)
plt.tight_layout()
plt.savefig('scripts/plots/total_time2.png', format='png', dpi=1000)
with PdfPages('scripts/plots/total_time2.pdf') as pdf:
    pdf.savefig(fig)
    print('generated total execution time plot 2 at scripts/plots/total_time2.pdf')

plt.clf()

# Query throughput
virtuo_quota = compute_throughput('results/watdiv-virtuo-quota-load', clients)
virtuo_pages = compute_throughput('results/watdiv-virtuo-pages-load', clients)
virtuo_inf = compute_throughput('results/watdiv-virtuoso-load', clients)
sage_150ms = compute_throughput('results/watdiv-sage-150ms', clients)
sage_75ms = compute_throughput('results/watdiv-sage-75ms', clients)
brtpf = compute_throughput('results/watdiv-brtpf-load', [1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 75, 100])
tpf = compute_throughput('results/watdiv-ref-load', clients, 1)

plt.rcParams["figure.figsize"] = [5, 5]
plt.rcParams["legend.columnspacing"] = 1.0
fig = plt.figure()
plt.rc('text', usetex=True)
ax = plt.axes(yscale='log')
ax.grid()
plt.plot(clients, virtuo_quota, linestyle='--', marker='o', color='b', label='VQ')
plt.plot(clients, virtuo_pages, linestyle='--', marker='.', color='m', label='VQP')
plt.plot(clients, virtuo_inf, linestyle='-', marker='o', color='r', label='VNQ')
plt.plot(clients, sage_150ms, linestyle='-', marker='X', color='y', label='SaGe-150')
plt.plot(clients, sage_75ms, linestyle='-', marker='X', color='c', label='SaGe-75')
plt.plot([1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 75, 100], brtpf, linestyle='-', marker='D', color='k', label='BrTPF')
plt.plot(clients, tpf, linestyle='-', marker='s', color='g', label='TPF')
plt.legend(ncol=4, loc='upper center', bbox_to_anchor=(0.5, 1.2), fontsize=13, fancybox=False)
plt.xlabel('Number of clients', fontsize=17)
plt.ylabel('Query throughput (q/hr)', fontsize=17)
plt.tick_params(axis='both', which='major', labelsize=15)
plt.tight_layout()
plt.savefig('scripts/plots/throughput.png', format='png', dpi=1000)
with PdfPages('scripts/plots/throughput.pdf') as pdf:
    pdf.savefig(fig)
    print('generated throughput plot at scripts/plots/throughput.pdf')

plt.clf()

# Query throughput 4 workers
virtuo_quota_4w = compute_throughput('results/watdiv-virtuoso4w-load', clients)
virtuo_quota_1w = compute_throughput('results/watdiv-virtuoso-load', clients)
sage_75ms_4w = compute_throughput('results/watdiv-sage-4w', clients)
sage_75ms_1w = compute_throughput('results/watdiv-sage-150ms', clients)
plt.rcParams["figure.figsize"] = [5, 3]
plt.rcParams["legend.columnspacing"] = 0.8
fig = plt.figure()
plt.rc('text', usetex=True)
ax = plt.axes(yscale='log')
ax.grid()
plt.plot(clients, virtuo_quota_4w, linestyle='-', marker='P', color='r', label='VNQ 4w')
plt.plot(clients, virtuo_quota_1w, linestyle='--', marker='x', color='r', label='VNQ 1w')
plt.plot(clients, sage_75ms_4w, linestyle='-', marker='P', color='c', label='SaGe-75 4w')
plt.plot(clients, sage_75ms_1w, linestyle='--', marker='x', color='c', label='SaGe-75 1w')
plt.legend(ncol=2, fontsize=13, fancybox=False)
plt.xlabel('Number of clients', fontsize=17)
plt.ylabel('Query throughput (q/hr)', fontsize=17)
plt.tick_params(axis='both', which='major', labelsize=15)
plt.tight_layout()
plt.savefig('scripts/plots/throughput2.png', format='png', dpi=1000)
with PdfPages('scripts/plots/throughput2.pdf') as pdf:
    pdf.savefig(fig)
    print('generated throughput plot 2 at scripts/plots/throughput2.pdf')

plt.clf()

# Query timeout
virtuo_quota = compute_virtuo_timeouts('results/watdiv-virtuo-quota-load', clients)
virtuo_pages = compute_timeouts('results/watdiv-virtuo-pages-load', clients)
virtuo_inf = compute_timeouts('results/watdiv-virtuoso-load', clients)
sage_150ms = compute_timeouts('results/watdiv-sage-150ms', clients)
sage_75ms = compute_timeouts('results/watdiv-sage-75ms', clients)
brtpf = compute_timeouts('results/watdiv-brtpf-load', [1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 75, 100])
tpf = compute_timeouts('results/watdiv-ref-load', clients, 1)

plt.rcParams["figure.figsize"] = [5, 5]
plt.rcParams["legend.columnspacing"] = 1.0
fig = plt.figure()
plt.rc('text', usetex=True)
ax = plt.axes()
ax.grid()
plt.plot(clients, virtuo_quota, linestyle='--', marker='o', color='b', label='VQ')
plt.plot(clients, virtuo_pages, linestyle='--', marker='.', color='m', label='VQP')
plt.plot(clients, virtuo_inf, linestyle='-', marker='o', color='r', label='VNQ')
plt.plot(clients, sage_150ms, linestyle='-', marker='X', color='y', label='SaGe-150')
plt.plot(clients, sage_75ms, linestyle='-', marker='X', color='c', label='SaGe-75')
plt.plot([1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 75, 100], brtpf, linestyle='-', marker='D', color='k', label='BrTPF')
plt.plot(clients, tpf, linestyle='-', marker='s', color='g', label='TPF')
plt.legend(ncol=4, loc='upper center', bbox_to_anchor=(0.5, 1.2), fontsize=13, fancybox=False)
plt.xlabel('Number of clients', fontsize=17)
plt.ylabel('Query timeout (\%)', fontsize=17)
plt.tick_params(axis='both', which='major', labelsize=15)
plt.tight_layout()
plt.savefig('scripts/plots/timeouts.png', format='png', dpi=1000)
with PdfPages('scripts/plots/timeouts.pdf') as pdf:
    pdf.savefig(fig)
    print('generated timeout plot at scripts/plots/timeouts.pdf')

plt.clf()

# Query timeout 2
virtuo_quota_4w = compute_timeouts('results/watdiv-virtuoso4w-load', clients)
virtuo_quota_1w = compute_timeouts('results/watdiv-virtuoso-load', clients)
sage_75ms_4w = compute_timeouts('results/watdiv-sage-4w', clients)
sage_75ms_1w = compute_timeouts('results/watdiv-sage-150ms', clients)

plt.rcParams["figure.figsize"] = [5, 3]
plt.rcParams["legend.columnspacing"] = 0.8
fig = plt.figure()
plt.rc('text', usetex=True)
ax = plt.axes()
ax.grid()
plt.plot(clients, virtuo_quota_4w, linestyle='-', marker='P', color='r', label='VNQ 4w')
plt.plot(clients, virtuo_quota_1w, linestyle='--', marker='x', color='r', label='VNQ 1w')
plt.plot(clients, sage_75ms_4w, linestyle='-', marker='P', color='c', label='SaGe-75 4w')
plt.plot(clients, sage_75ms_1w, linestyle='--', marker='x', color='c', label='SaGe-75 1w')
plt.legend(ncol=1, fontsize=13, fancybox=False)
plt.xlabel('Number of clients', fontsize=17)
plt.ylabel('Query timeout (\%)', fontsize=17)
plt.tick_params(axis='both', which='major', labelsize=15)
plt.tight_layout()
plt.savefig('scripts/plots/timeouts2.png', format='png', dpi=1000)
with PdfPages('scripts/plots/timeouts2.pdf') as pdf:
    pdf.savefig(fig)
    print('generated timeout plot2 at scripts/plots/timeouts2.pdf')

plt.clf()

# Completeness
virtuo_quota = compute_virtuo_completeness('results/watdiv-virtuoso-load', 'results/watdiv-virtuo-quota-load', clients)
virtuo_pages = compute_virtuo_completeness('results/watdiv-virtuoso-load', 'results/watdiv-virtuo-pages-load', clients)
virtuo_inf = compute_virtuo_completeness('results/watdiv-virtuoso-load', 'results/watdiv-virtuoso-load', clients)
sage_75ms = compute_completeness('results/watdiv-sage-75ms', clients)

plt.rcParams["figure.figsize"] = [5, 4]
plt.rcParams["legend.columnspacing"] = 1.0
fig = plt.figure()
plt.rc('text', usetex=True)
ax = plt.axes()
ax.grid()
plt.plot(clients, virtuo_quota, linestyle='--', marker='o', color='b', label='VQ')
# plt.plot(clients, virtuo_pages, linestyle='--', marker='s', color='r', label='VQP')
plt.plot(clients, virtuo_inf, linestyle='-', marker='o', color='r', label='VNQ')
plt.plot(clients, sage_75ms, linestyle='-', marker='X', color='c', label='SaGe')
# plt.plot(clients, brtpf, linestyle='-', marker='H', color='y', label='BrTPF')
# plt.plot(clients, tpf, linestyle='-', marker='s', color='g', label='TPF')
plt.legend(fontsize=13)
plt.xlabel('Number of clients', fontsize=17)
plt.ylabel('Workload completeness (\%)', fontsize=17)
plt.tick_params(axis='both', which='major', labelsize=15)
plt.tight_layout()
plt.savefig('scripts/plots/completeness.png', format='png', dpi=1000)
with PdfPages('scripts/plots/completeness.pdf') as pdf:
    pdf.savefig(fig)
    print('generated timeout plot at scripts/plots/completeness.pdf')

plt.clf()

# http requests
virtuo_quota = compute_nb_http('results/watdiv-virtuo-quota-load', clients)
virtuo_pages = compute_nb_http('results/watdiv-virtuo-pages-load', clients)
virtuo_inf = compute_nb_http('results/watdiv-virtuoso-load', clients)
sage_75ms = compute_nb_http('results/watdiv-sage-75ms', clients)
sage_150ms = compute_nb_http('results/watdiv-sage-150ms', clients)
brtpf = compute_nb_http('results/watdiv-brtpf-load', [1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 75, 100])
tpf = compute_nb_http('results/watdiv-ref-load', clients, 1)

plt.rcParams["figure.figsize"] = [5, 4]
plt.rcParams["legend.columnspacing"] = 1.0
fig = plt.figure()
plt.rc('text', usetex=True)
ax = plt.axes(yscale='log')
ax.grid()
plt.plot(clients, virtuo_quota, linestyle='--', marker='o', color='b', label='VQ')
plt.plot(clients, virtuo_pages, linestyle='--', marker='.', color='m', label='VQP')
plt.plot(clients, virtuo_inf, linestyle='-', marker='o', color='r', label='VNQ')
plt.plot(clients, sage_150ms, linestyle='-', marker='X', color='y', label='SaGe-150')
plt.plot(clients, sage_75ms, linestyle='-', marker='X', color='c', label='SaGe-75')
plt.plot([1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 75, 100], brtpf, linestyle='-', marker='D', color='k', label='BrTPF')
plt.plot(clients, tpf, linestyle='-', marker='s', color='g', label='TPF')
plt.legend(ncol=3, fontsize=13, bbox_to_anchor=(1.0, 0.5))
plt.xlabel('Number of clients', fontsize=17)
plt.ylabel('avg. number of HTTP requests', fontsize=17)
plt.tick_params(axis='both', which='major', labelsize=15)
plt.tight_layout()
plt.savefig('scripts/plots/http_requests.png', format='png', dpi=1000)
with PdfPages('scripts/plots/http_requests.pdf') as pdf:
    pdf.savefig(fig)
    print('generated HTTP requests plot at scripts/plots/http_requests.pdf')

plt.clf()

# overhead
sage_150ms = compute_overhead('results/watdiv-sage-150ms', clients)
sage_75ms = compute_overhead('results/watdiv-sage-75ms', clients)

plt.rcParams["figure.figsize"] = [5, 4]
plt.rcParams["legend.columnspacing"] = 1.0
fig = plt.figure()
plt.rc('text', usetex=True)
ax = plt.axes()
ax.grid()
plt.plot(clients, sage_150ms, linestyle='-', marker='X', color='y', label='SaGe-150')
plt.plot(clients, sage_75ms, linestyle='-', marker='X', color='c', label='SaGe-75')
# plt.plot(clients, tpf, linestyle='-', marker='h', color='m', label='TPF')
plt.legend(fontsize=17)
plt.xlabel('Number of clients', fontsize=17)
plt.ylabel('avg. preemption\noverhead (ms)', fontsize=17)
plt.tick_params(axis='both', which='major', labelsize=15)
plt.yticks(np.arange(0, 0.6, step=0.1))
plt.tight_layout()
plt.savefig('scripts/plots/overhead.png', format='png', dpi=1000)
with PdfPages('scripts/plots/overhead.pdf') as pdf:
    pdf.savefig(fig)
    print('generated overhead plot at scripts/plots/overhead.pdf')
