import numpy as np
from os import listdir
from matplotlib.backends.backend_pdf import PdfPages

CLIENTS = [1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 95, 100]

NB_QUERIES_PER_MIX = 194
DELTA = 0.05
DATA_SIZES = {
    "sage": 87,
    "tpf": 4
}


class DrawPlot(object):
    """Context manager used to draw plots with matplotlib"""

    def __init__(self, name, plt, xlabel, ylabel, yscale='log', save_png=False):
        super(DrawPlot, self).__init__()
        self._name = name
        self._plt = plt
        self._xlabel = xlabel
        self._ylabel = ylabel
        self._yscale = yscale
        self._save_png = save_png

    def __enter__(self):
        self._figure = self._plt.figure()
        self._plt.rc('text', usetex=True)
        ax = self._plt.axes(yscale=self._yscale)
        ax.grid()
        return self._plt

    def __exit__(self, type, value, traceback):
        self._plt.xlabel(self._xlabel, fontsize=17)
        self._plt.ylabel(self._ylabel, fontsize=17)
        self._plt.tick_params(axis='both', which='major', labelsize=15)
        self._plt.tight_layout()
        if self._save_png:
            self._plt.savefig('scripts/plots/{}.png'.format(self._name), format='png', dpi=1000)
        with PdfPages('scripts/plots/{}.pdf'.format(self._name)) as pdf:
            pdf.savefig(self._figure, bbox_inches='tight')
            print('generated {} plot at scripts/plots/{}.pdf'.format(self._name, self._name))

        self._plt.clf()


def compute_delta_time(base_path, clients, nbRuns=3, add_latency=False):
    """
        Compute Delta_x = E_x - E_0, where:
        - E_O is the execution time with no concurrent clients
        - E_x is the execution time with x concurrent clients

        E_x = E_0 + alpha * n
        alpha * n = E_x - E_0
    """

    def get_delta(run):
        results = [0]
        base_times = dict()
        for nb_clients in clients:
            df = np.genfromtxt('{}/run{}/execution_times_{}c.csv'.format(base_path, run, nb_clients),
                               delimiter=',', names=True, dtype=None, encoding='utf-8', invalid_raise=False)
            if nb_clients == 1:
                for row in df:
                    base_times[row['query']] = row['time'] + (row['httpCalls'] * 0.05) if add_latency else row['time']
            else:
                tmp = []
                for row in df:
                    t = (row['time'] + (row['httpCalls'] * 0.05)) if add_latency else row['time']
                    value = t - base_times[row['query']]
                    if value <= 0:
                        tmp.append(0)
                    else:
                        tmp.append(value)
                results.append(np.mean(tmp))
        return results

    deltas = [get_delta(x) for x in range(1, nbRuns + 1)]
    res = []
    for ind in range(len(deltas[0])):
        res.append(np.mean([row[ind] for row in deltas]))
    return res


def compute_avg_value(fn):
    """Get a function used to compute an average value over several dataframe"""
    def process(basePath, clients, nbRuns, suffix="sage"):
        res = []
        for nb in clients:
            values = []
            for x in range(2, nbRuns + 1):
                directory = "{}/run{}/{}clients/".format(basePath, x, nb)
                res_directories = listdir(directory)
                dataframes = []
                for res_dir in res_directories:
                    df = np.genfromtxt('{}/{}/execution_times_{}.csv'.format(directory, res_dir, suffix), delimiter=',', names=True, dtype=None, encoding='utf-8', invalid_raise=False)
                    dataframes += [(df, NB_QUERIES_PER_MIX - len(df))]
                values += [fn(dataframes, nb)]
            res += [np.mean(values)]
        return res
    return process


def compute_total_time(basePath, clients, nbRuns=3, add_latency=False, suffix="sage"):
    """Compute average query throughput"""
    def processor(dataframes, nb_clients):
        def mapper(x):
            if not add_latency:
                return x['time']
            return x['time'] + (x['httpCalls'] * DELTA)

        res = []
        timeouts = 0
        for (df, nb_timeout) in dataframes:
            res += list(map(mapper, df))
            timeouts += nb_timeout
        return (np.sum(res) + timeouts * 120) / (nb_clients)
    return compute_avg_value(processor)(basePath, clients, nbRuns, suffix=suffix)


def compute_throughput(basePath, clients, nbRuns=3, add_latency=False, suffix="sage"):
    """Compute average query throughput"""
    def processor(dataframes, nb_clients):
        def mapper(x):
            if not add_latency:
                return x['time']
            return x['time'] + (x['httpCalls'] * DELTA)

        res = []
        nb_timeouts = 0
        for (df, nb_timeout) in dataframes:
            res += [np.sum(list(map(mapper, df))) + (nb_timeout * 120)]
            nb_timeouts += nb_timeout
        return max(res)
    return compute_avg_value(processor)(basePath, clients, nbRuns, suffix=suffix)


def compute_nb_http(basePath, clients, nbRuns=3, suffix="sage"):
    """Compute avg. number of HTTP requests"""
    def processor(dataframes, nb_clients):
        res = 0
        for (df, nb_timeout) in dataframes:
            res += np.sum(df['httpCalls'])
        return res
    return compute_avg_value(processor)(basePath, clients, nbRuns, suffix=suffix)


def compute_data_transfers(basePath, clients, nbRuns=3, suffix="sage"):
    """Compute avg. number of HTTP requests"""
    def processor(dataframes, nb_clients):
        res = 0
        for (df, nb_timeout) in dataframes:
            res += np.sum(df['httpCalls'] * DATA_SIZES[suffix])
        return res / 1000
    return compute_avg_value(processor)(basePath, clients, nbRuns, suffix=suffix)


def compute_overhead(basePath, clients, nbRuns=3, metric="importTime", suffix="sage"):
    """Compute avg. preemption overhead"""
    def processor(dataframes, nb_clients):
        res = []
        for (df, nb_timeout) in dataframes:
            res += list(df[metric])
        return np.mean(res)
    return compute_avg_value(processor)(basePath, clients, nbRuns,  suffix=suffix)


def compute_tffr(basePath, clients, nbRuns=3, suffix="sage"):
    """Compute avg. time for first results"""
    metric = "time"
    delta = 0
    if suffix == "sage":
        metric = "serverTime"
        delta = DELTA

    def processor(dataframes, nb_clients):
        res = []
        for (df, nb_timeout) in dataframes:
            res += list(df[metric] + delta)
        return np.mean(res)
    return compute_avg_value(processor)(basePath, clients, nbRuns,  suffix=suffix)
