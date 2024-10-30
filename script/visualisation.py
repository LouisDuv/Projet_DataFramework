import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np


def bar_plot(x, y):

    x_n = x.to_numpy()
    y_n = y.to_numpy()

    x_fig_size = int(len(x_n) - len(x_n) * 0.1)
    print(x_fig_size)

    colors = ["blue" if value > 0 else "red" for value in y_n]

    plt.figure(figsize=(x_fig_size, 6))
    plt.bar(x_n, y_n, color = colors)
    plt.title(f"{y.name}")
    plt.xticks(rotation=70)

    plt.legend()
    plt.show()