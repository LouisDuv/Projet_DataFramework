import matplotlib.pyplot as plt
import numpy as np
import streamlit as st
import matplotlib.dates as mdates
from pandas import Series

from script.DataFrame import DataframeClass


st.set_page_config(
    page_title="Home Page",
    layout= "wide" 
)

def home_page(x, y, type_graph : str):
    
    left_container, center_container, right_container = st.columns([2, 5, 3])
    with left_container: 
        st.title('Actions')
        
        with st.container() :
            st.button("Average Return", type="secondary", key =  'avg_ret')
        with st.container() :
            st.button("Return Rate", type="secondary", key='rrate')
        with st.container() :
            st.button("Stock Variation", type="secondary", key = 'stock_var')
        with st.container() :
            st.button("Dividend Simulator", type="secondary", key = 'div_sim')
        with st.container() :
            st.button("Correlation", type="secondary", key = 'corr')

    with center_container:
        st.title(f'About the compagny')
        with st.container(key='graph_container') :
            graph_container(x, y, type_graph)
        with st.container(key = 'information') :
            st.markdown('INSIGHTS' * 15)
    with right_container :
        option = st.selectbox(
            "Choose another stocks",
            ("AAPL", "Microsoft", "PFIZER", "FORD"),
        )

def graph_container(x, y, type_graph = str):

    if type_graph == 'l':
        st.markdown('CENTER_GRAPH_LINEAR' * 40)
    elif type_graph == 'b' :
        st.markdown('CENTER_BAR_PLOT' * 40)


def bar_plot(x, y):

    colors = ["blue" if value > 0 else "red" for value in y]

    fig, axes = plt.subplot(1, 1)
    axes.bar(x, y, color = colors, width=14)
    axes.set_title(f"{y.name}")

    plt.title('Title')
    st.pyplot(fig)

def linear_plot(x , y, color_used : str = "blue"):
    
    fig, axes = plt.subplots(1, 1,)
    axes.plot(x, y, color = color_used)
    st.pyplot(fig)
    
def scatter_plot(x, y, x_name = None, y_name = None, title=None):
    
    plt.scatter(x, y)

    plt.title
    plt.xlabel(x_name)
    plt.ylabel(y_name)

    plt.legend()
    plt.show()

def streamlit_test(df):
    st.write(df)

