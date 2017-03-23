#!/usr/bin/env python
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# Import data into panda dataframe
latency_ratio = pd.read_csv("perflow_global_fcfs.csv",
                            index_col='cores')

# Remove Index Title
latency_ratio.index.name = "# cores"

columns_labels = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '20',
                  '30', '40', '50', '60', '70', '80', '90', '100', '200',
                  '300', '400', '500', '600', '700', '800', '900', '1000']

row_labels = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '15', '20',
              '25', '50', '100', '250', '500', '750', '1000']


# Set appropriate font and dpi
sns.set(font_scale=2.5)
sns.set_style({"savefig.dpi": 100})

# Plot the heatmap
ax = sns.heatmap(latency_ratio, cmap=plt.cm.PuOr,
                 linewidth=.1, vmin=0.0, vmax=2.0)

# Set the x-axis label and title
ax.set_xlabel("Flow 2 mean / Flow 1 mean")
ax.set_title("Per Flow Queue Latency / Global Queue Latency for Flow 1")

# Create the figure
fig = ax.get_figure()

# Specify dimensions and save
fig.set_size_inches(25, 15)
fig.savefig("perflow_global_fcfs.png")
