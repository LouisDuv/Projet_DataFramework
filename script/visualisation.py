import matplotlib.pyplot as plt
import numpy as np
import streamlit as st
import matplotlib.dates as mdates



def period_to_string(string) :
    if string == "w" :
        return "Weekly"
    elif string == "m":
        return "Monthly"
    elif string == "y":
        return "Yearly"

    else : 
        print("[INFO] -> Mistake in the given string ")
        return -1

# Bar Charts
# X d
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

def linear_plot(x : list, y : list, period : str, str : str = None, color_used : str = "blue"):
    
    fig_xshape = 14

    if len(x) > 90 :
        ticks_split = 12
        fig_xshape = 14
    elif 60 <= len(x) <= 90:
        ticks_split = 5
    elif 30 <= len(x) < 60 :
        ticks_split = 3
    elif 15 <= len(x) <=30 :
        ticks_split = 2
    else :
        ticks_split = 1

    fig, axes = plt.subplots(figsize = (fig_xshape, 6))

    axes.set_xticks(x[::ticks_split]) # Afficher des ticks tous les 3 mesures 

    axes.set_xticklabels([date for date in x[::ticks_split]])
    axes.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    
    axes.plot(x, y, marker = "o", color =  color_used, alpha = .5 )
    axes.set_xlabel(x.name)
    axes.set_ylabel(y.name)

    axes.tick_params(axis='x', rotation=50)

    plt.title(f"{period_to_string(period)}_Average_{str}_Price_($)")
    plt.show()

def scatter_plot(x, y, x_name = None, y_name = None, title=None):
    plt.scatter(x, y)

    plt.title
    plt.xlabel(x_name)
    plt.ylabel(y_name)

    plt.legend()
    plt.show()

def streamlit_test(df):
    st.write(df)