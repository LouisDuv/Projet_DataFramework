import matplotlib.pyplot as plt
import numpy as np


def bar_plot(x, y):

    x_n = x.to_numpy()
    y_n = y.to_numpy()

    colors = ["blue" if value > 0 else "red" for value in y_n]

    plt.figure(figsize=(12, 6))
    plt.bar(x_n, y_n, color = colors, width=14)
    plt.title(f"{y.name}")
    plt.xticks(rotation=70)

    plt.xlabel("Date")
    plt.ylabel("Variation")
    plt.legend()
    plt.show()

def scatter_plot(x, y, x_name = None, y_name = None, title=None):
    plt.scatter(x, y)

    plt.title
    plt.xlabel(x_name)
    plt.ylabel(y_name)

    plt.legend()
    plt.show()