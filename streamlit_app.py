import streamlit as st
import matplotlib.pyplot as plt

import os
os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"
import glob

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

from DataFrame import DataframeClass
from FinancialDataAnalyzer import FinancialDataAnalyzer


spark = SparkSession.builder.appName("StockAnalysis").config("spark.driver.host", "localhost").getOrCreate()
dataframe_obj = DataframeClass(spark)
analyzer = FinancialDataAnalyzer()
csv_folder_path = 'Stocks_Price'
csv_files = glob.glob(os.path.join(csv_folder_path, "*.csv"))
data_dfs = dataframe_obj.read_multiple_csv(csv_files)




@st.cache_data
def df_to_csv(df):
    return df.to_csv().encode("utf-8")


def first_type_st_layout(operation, opr: str):
    
    col1, col2 = st.columns([1, 2], gap="large")

    with col1:
        selected_dataset_name = st.selectbox(
            "📁 Choose a dataset:",
            options=["Select a dataset"] + dataframe_obj.datasets_names
        )

    with col2:
        if selected_dataset_name != "Select a dataset":
            dataset_idx = dataframe_obj.datasets_names.index(selected_dataset_name)
            result = dataframe_obj.perform_operation(dataset_idx, operation, selected_dataset_name)
            st.write("📊 Result:")
            if isinstance(result, int):  
                st.write(f"✅ {result}")
            else:  
                st.dataframe(result)
                st.write("Download data as CSV:")
                to_download = df_to_csv(result)
                st.download_button(
                    label="💾",
                    data=to_download,
                    file_name=f"{selected_dataset_name}_{opr}.csv",
                    mime="text/csv",
                )


def second_type_st_layout(operation, opr: str):
    
    col1, col2 = st.columns([1, 2], gap="large")
    
    with col1:
        selected_dataset_name = st.selectbox(
            "📁 Choose a dataset:",
            options=["Select a dataset"] + dataframe_obj.datasets_names
        )
        first_choice = "Select a column" 
        second_choice = "Select a column" 
        filtered_columns = [] 
        if selected_dataset_name != "Select a dataset":
            dataset_idx = dataframe_obj.datasets_names.index(selected_dataset_name)
            selected_dataframe = dataframe_obj.dataframes[dataset_idx]
            available_columns = [col for col in selected_dataframe.columns if col != "Date"]
            first_choice = st.selectbox("Choose the first value", options=["Select a column"] + available_columns)
            if first_choice != "Select a column":
                filtered_columns = [col for col in available_columns if col != first_choice]
            if filtered_columns:
                second_choice = st.selectbox("Choose the second value", options=["Select a column"] + filtered_columns)

    with col2:
        if (first_choice != "Select a column") & (second_choice != "Select a column"):
            result = dataframe_obj.perform_operation(dataset_idx, operation, first_choice, second_choice, selected_dataset_name)
            st.write("📊 Result:")
            if isinstance(result, (int, float)):  
                st.write(f"✅ {result}")
            else:  
                st.dataframe(result)
                st.write("Download data as CSV:")
                to_download = df_to_csv(result)
                st.download_button(
                    label="💾",
                    data=to_download,
                    file_name=f"{selected_dataset_name}_{opr}.csv",
                    mime="text/csv",
                )


def third_type_st_layout(operation, opr: str):
    
    col1, col2 = st.columns([1, 2], gap="large")
    
    with col1:
        selected_dataset_name = st.selectbox(
            "📁 Choose a dataset:",
            options=["Select a dataset"] + dataframe_obj.datasets_names
        )
        first_choice = "Select an option" 
        second_choice = "Select an option" 
        if selected_dataset_name != "Select a dataset":
            dataset_idx = dataframe_obj.datasets_names.index(selected_dataset_name)
            first_choice = st.selectbox("Choose the value", options=["Select an option"] + ["Open", "Close"])
            if first_choice != "Select a column":
                second_choice = st.selectbox("Choose the period", options=["Select an option"] + ["week", "month", "year"])

    with col2:
        if (first_choice != "Select an option") & (second_choice != "Select an option"):
            result = dataframe_obj.perform_operation(dataset_idx, operation, first_choice, second_choice, selected_dataset_name)
            st.write("📊 Result:")
            if isinstance(result, (int, float)):  
                st.write(f"✅ {result}")
            else:  
                st.dataframe(result)
                st.write("Download data as CSV:")
                to_download = df_to_csv(result)
                st.download_button(
                    label="💾",
                    data=to_download,
                    file_name=f"{selected_dataset_name}_{opr}.csv",
                    mime="text/csv",
                )


def fourth_type_st_layout(operation, opr: str, period: list):
    
    col1, col2 = st.columns([1, 2], gap="large")
    
    with col1:
        selected_dataset_name = st.selectbox(
            "📁 Choose a dataset:",
            options=["Select a dataset"] + dataframe_obj.datasets_names
        )
        first_choice = "Select an option" 
        if selected_dataset_name != "Select a dataset":
            dataset_idx = dataframe_obj.datasets_names.index(selected_dataset_name)
            first_choice = st.selectbox("Choose the period", options=["Select an option"] + period)

    with col2:
        if first_choice != "Select an option":
            result = dataframe_obj.perform_operation(dataset_idx, operation, first_choice, selected_dataset_name)
            st.write("📊 Result:")
            if isinstance(result, (int, float)):  
                st.write(f"✅ {result}")
            else:  
                st.dataframe(result)
                st.write("Download data as CSV:")
                to_download = df_to_csv(result)
                st.download_button(
                    label="💾",
                    data=to_download,
                    file_name=f"{selected_dataset_name}_{opr}.csv",
                    mime="text/csv",
                )


def fifth_type_st_layout(operation, opr: str):
    
    col1, col2 = st.columns([1, 2], gap="large")
    
    with col1:
        selected_dataset_name = st.selectbox(
            "📁 Choose a dataset:",
            options=["Select a dataset"] + dataframe_obj.datasets_names
        )
        first_choice = "Select a column" 
        second_choice = "Select a column" 
        if selected_dataset_name != "Select a dataset":
            dataset_idx = dataframe_obj.datasets_names.index(selected_dataset_name)
            selected_dataframe = dataframe_obj.dataframes[dataset_idx]
            available_columns = [col for col in selected_dataframe.columns if col != "Date"]
            first_choice = st.selectbox("Choose the first value", options=["Select a column"] + available_columns)
            if first_choice != "Select a column":
                second_choice = st.number_input("Choose a period", 1, selected_dataframe.count(), 1)
                if st.button("Validate Selection"):
                    st.session_state['validated'] = True
                else:
                    st.session_state['validated'] = False

    with col2:
        if (first_choice != "Select a column") & st.session_state.get('validated', False):
            result = dataframe_obj.perform_operation(dataset_idx, operation, first_choice, second_choice, selected_dataset_name)
            st.write("📊 Result:")
            if isinstance(result, (int, float)):  
                st.write(f"✅ {result}")
            else:  
                st.dataframe(result)
                st.write("Download data as CSV:")
                to_download = df_to_csv(result)
                st.download_button(
                    label="💾",
                    data=to_download,
                    file_name=f"{selected_dataset_name}_{opr}.csv",
                    mime="text/csv",
                )
                
                
def sixth_type_st_layout(operation, opr: str):
    
    col1, col2 = st.columns([1, 2], gap="large")
    
    with col1:
        selected_dataset_name1 = st.selectbox(
            "📁 Choose the first dataset:",
            options=["Select a dataset"] + dataframe_obj.datasets_names
        )
        selected_dataset_name2 = st.selectbox(
            "📁 Choose the second dataset:",
            options=["Select a dataset"] + dataframe_obj.datasets_names
        )

    with col2:
        first_value = "Select a column" 
        second_value = "Select a column" 
        if selected_dataset_name1 != "Select a dataset":
            dataset_idx1 = dataframe_obj.datasets_names.index(selected_dataset_name1)
            selected_dataframe1 = dataframe_obj.dataframes[dataset_idx1]
            available_columns1 = [col for col in selected_dataframe1.columns if col != "Date"]
            first_value = st.selectbox("Choose the first value", options=["Select a column"] + available_columns1)
        if selected_dataset_name2 != "Select a dataset":
            dataset_idx2 = dataframe_obj.datasets_names.index(selected_dataset_name2)
            selected_dataframe2 = dataframe_obj.dataframes[dataset_idx2]
            available_columns2 = [col for col in selected_dataframe2.columns if col != "Date"]
            second_value = st.selectbox("Choose the second value", options=["Select a column"] + available_columns2)
        if (first_value != "Select a column") & (second_value != "Select a column"):
            # result = dataframe_obj.perform_operation(dataset_idx, operation, first_choice, second_choice, selected_dataset_name)
            result = operation(selected_dataframe1, selected_dataframe2, first_value, second_value, selected_dataset_name1, selected_dataset_name2)
            st.write("📊 Result:")
            if isinstance(result, (int, float)):  
                st.write(f"✅ {result}")
            else:  
                st.dataframe(result)
                st.write("Download data as CSV:")
                to_download = df_to_csv(result)
                st.download_button(
                    label="💾",
                    data=to_download,
                    file_name=f"{selected_dataset_name1}_{selected_dataset_name2}_{opr}.csv",
                    mime="text/csv",
                )


functions_selection_mapping = {
    "Show Head and Tail (40 rows)": analyzer.head_and_tail_40,
    "Show Number of Observations": analyzer.num_observations,
    "Get Period Between Data Points": analyzer.period_btw_data,
    "Get Descriptive Statistics": analyzer.descript_stats,
    "Count Missing Values": analyzer.count_missing,
    "Get Correlation Between Values": analyzer.values_correlation,
    "Get Opening's / Closing's Price Average": analyzer.avg_price_period,
    "Show Stock's Variation": analyzer.stock_variation,
    "Get Average Daily Return / Period": analyzer.period_return,
    "Get Stock With Highest Daily Return": analyzer.max_return,
    "Show Moving Average": analyzer.moving_average,
    "Get Correlation Between Stocks": analyzer.correlation_btw_stocks,
    "Get Return Rate": analyzer.return_rate,
    "Get Stock With Best Return Rate": analyzer.max_return_rate
}

st.sidebar.title("🔧 Operations")
col_opr = st.columns(1)
with col_opr[0]:
    options = list(functions_selection_mapping.keys())
    function_selected = st.sidebar.pills("Choose an action:", options, selection_mode="single")

if function_selected == None:
    
    st.title("Welcome to ECE-Analysis")
    st.image(".img\dataGraphs_app.png", use_container_width=True)
    
    total_datasets = len(dataframe_obj.dataframes)
    st.metric(label="📂 Total Datasets Loaded", value=total_datasets)
    
    st.markdown(
        """
        ### 📏 Explore and Analyze Stock Data with Ease
        - Choose an action to analyze the selected dataset.
        - Select datasets to be analysed through the scope of this function.
        
        #### 📝 Features
        - Compare multiple stock datasets.
        - View detailed statistics and visualizations.
        - Download processed results.
        
        ### 📚 Usage Instructions
        1. **Select a Dataset**: Choose from the available stock datasets.
        2. **Select an Operation**: Apply an analysis function to the dataset.
        3. **View Results**: Results are displayed below.
        """
    )
    
    st.info("📌 Please select an operation from the sidebar to proceed.")

    
else:   
    
    st.title(function_selected)
    
    if function_selected == "Show Head and Tail (40 rows)":
        first_type_st_layout(functions_selection_mapping["Show Head and Tail (40 rows)"], "head_and_tail_40")

    elif function_selected == "Show Number of Observations":
        first_type_st_layout(functions_selection_mapping["Show Number of Observations"], "number_of_observations")
        
    elif function_selected == "Get Period Between Data Points":
        first_type_st_layout(functions_selection_mapping["Get Period Between Data Points"], "period_btw_data_points")
    
    elif function_selected == "Get Descriptive Statistics":
        first_type_st_layout(functions_selection_mapping["Get Descriptive Statistics"], "descriptive_stats")
        
    elif function_selected == "Count Missing Values":
        first_type_st_layout(functions_selection_mapping["Count Missing Values"], "count_missing_values")
        
    elif function_selected == "Get Correlation Between Values":
        second_type_st_layout(functions_selection_mapping["Get Correlation Between Values"], "correlation_btw_values")
        
    elif function_selected == "Get Opening's / Closing's Price Average":
        third_type_st_layout(functions_selection_mapping["Get Opening's / Closing's Price Average"], "period_price_average")
        
    elif function_selected == "Show Stock's Variation":
        fourth_type_st_layout(functions_selection_mapping["Show Stock's Variation"], "stock_price_variation", ["day", "month", "year"])
        
    elif function_selected == "Get Average Daily Return / Period":
        fourth_type_st_layout(functions_selection_mapping["Get Average Daily Return / Period"], "period_average_daily_return", ["day", "week", "month", "year"])
        
    elif function_selected == "Show Moving Average":
        fifth_type_st_layout(functions_selection_mapping["Show Moving Average"], "moving_average")
    
    elif function_selected == "Get Correlation Between Stocks":
        sixth_type_st_layout(functions_selection_mapping["Get Correlation Between Stocks"], "correlation_btw_stocks")
        
    elif function_selected == "Get Return Rate":
        hfv
        
    elif function_selected == "Get Stock With Best Return Rate":
        uyqgd



