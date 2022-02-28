# Utility program to plot timing data per
# per process on Chord Rings from info stored by Chord application
# in csv files e.g. statistics.csv. The input file is given
# as a command line argument e.g. python stats.py statistics.csv

import sys
from pandas import read_csv
import matplotlib.pyplot as plt
from math import ceil

l = len(sys.argv)
df = read_csv(sys.argv[1], names=['stamp', 'operation', 'nodes', 'elapsed_time'], header=None)
grouped = df.groupby(['operation', 'nodes']).mean('elapsed_time').reset_index()

ops = grouped['operation'].unique()
ncols = 2
nrows = ceil(len(ops) / ncols)
fig, axes = plt.subplots(nrows=nrows, ncols=ncols, figsize=(12, 12))
plt.rcParams["figure.figsize"] = [7.50, 3.50]
plt.rcParams["figure.autolayout"] = True
k = 0
for op in ops:
    op_df = grouped[grouped['operation'] == op][['nodes', 'elapsed_time']]
    n = k % ncols
    m = int (k/ncols)
    axes[m][n].bar(op_df['nodes'], op_df['elapsed_time'], align="center", width=0.5, alpha=0.5)
    axes[m][n].set_title(op, color='red')
    axes[m][n].set_ylabel('Time in seconds')
    axes[m][n].set_xlabel('Number of Nodes')
    k += 1
fig.canvas.manager.set_window_title('Chord Statistics')
fig.tight_layout(pad=4.0)
plt.show()
