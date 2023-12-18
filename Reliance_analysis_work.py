from pymongo import MongoClient
import json
from datetime import datetime
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from statsmodels.tsa.seasonal import STL
import plotly.graph_objects as go
from dagster import asset
@asset
def script2():
    # Connect to MongoDB
    client = MongoClient("mongodb://%s:%s@127.0.0.1" % ("dap", "dap"))
    db = client["Reliance_stock"]
    collection_name = "DAP_PROJECT"

    # Load JSON data from the file
    with open("dataset\\reliance.json", "r") as file:
        data = json.load(file)

    # Extracting relevant data from the loaded JSON
    meta_data = data.get('Meta Data', {})
    symbol_key = meta_data.get('2. Symbol', 'Unknown')

    time_series_data = data.get('Time Series (Daily)', {})
    daily_data_list = []

    for date, values in time_series_data.items():
        # Convert string values to appropriate types
        values['1. open'] = float(values.get('1. open', 0))
        values['2. high'] = float(values.get('2. high', 0))
        values['3. low'] = float(values.get('3. low', 0))
        values['4. close'] = float(values.get('4. close', 0))
        values['5. volume'] = int(values.get('5. volume', 0))

        # Converting date string to datetime object
        date_obj = datetime.strptime(date, '%Y-%m-%d')
        values['date'] = date_obj
        values['symbol'] = symbol_key

        daily_data_list.append(values)

    # Inserting documents into MongoDB if they don't already exist
    for document in daily_data_list:
        date_to_insert = document['date']

        # Checking if the document with the same date already exists in the collection
        existing_document = db[collection_name].find_one({'date': date_to_insert})

        if existing_document is None:

            db[collection_name].insert_one(document)
            print(f"Data for date {date_to_insert} inserted successfully.")
        else:

            print(f"Data for date {date_to_insert} already exists. Skipping insertion.")

    # Query data from MongoDB
    cursor = db[collection_name].find({})
    data_list = list(cursor)

    # Convert data to Pandas DataFrame
    df = pd.json_normalize(data_list)

    # Convert the 'date' column to datetime
    df['date'] = pd.to_datetime(df['date'])

    # Print the head of the DataFrame
    print("Head of the DataFrame:")
    print(df.head())

    print("Column Names:")
    print(df.columns.tolist())

    # Exploratory data analysis
    # Checking for null values in the DataFrame
    null_values = df.isnull().sum()

    # Print the null values
    print("Null Values:", null_values)

    # print the number of rows and columns in the DataFrame
    num_rows, num_columns = df.shape

    # Print the number of rows and columns
    print("Number of Rows:", num_rows)
    print("Number of Columns:", num_columns)

    # Check the data types of each column
    data_types = df.dtypes

    # Print the data types
    print("Data Types:", data_types)

    # Converting non-numeric columns to numeric (excluding object types)
    df_numeric = df.select_dtypes(exclude=['object'])

    # Creating a correlation matrix for numeric columns
    correlation_matrix = df_numeric.corr()


    # Plot the heatmap
    plt.figure(figsize=(10, 8))
    sns.heatmap(correlation_matrix, annot=True, cmap="mako", fmt=".2f")
    plt.title("Correlation Matrix")
    plt.show()
    df1 = df.set_index('date')
    print("date as index:", df1)

    # Converting columns to numeric, excluding object types
    df1_numeric = df1.apply(pd.to_numeric, errors='coerce')

    # Dropping columns that couldn't be converted to numeric
    df1_numeric = df1_numeric.dropna(axis=1, how='all')

    # Resample to monthly frequency
    df2 = df1_numeric.resample('M').mean()

    # Printing the result DataFrame
    print("daily to monthly date as index:", df2)

    # time series decomposition for column high to check trend seasonality residual
    result = STL(df2['2. high'], seasonal=13).fit()
    trend_component = result.trend
    seasonal_component = result.seasonal
    residual_component = result.resid

    # Plot the components
    plt.figure(figsize=(12, 8))

    plt.subplot(4, 1, 1)
    plt.plot(trend_component,label='Trend', color='blue')
    plt.title('Trend Component')
    plt.legend()

    plt.subplot(4, 1, 2)
    plt.plot(seasonal_component, label='Seasonal', color='green')
    plt.title('Seasonal Component')
    plt.legend()

    plt.subplot(4, 1, 3)
    plt.plot(residual_component, label='Residual', color='orange')
    plt.title('Residual Component')
    plt.legend()

    plt.subplot(4, 1, 4)
    plt.plot(df2['2. high'], label='Original Data', color='black')
    plt.title('Original Data')
    plt.legend()

    plt.tight_layout()
    plt.show()


    # Line Chart for column high
    fig_line = go.Figure()
    fig_line.add_trace(go.Scatter(x=df2.index, y=df2['2. high'], mode='lines', name='High Price'))
    fig_line.update_layout(title='Stock Market Time Series - High Price',
                          xaxis_title='date',
                          yaxis_title='High Price',
                          template='plotly_dark')

    fig_line.update_traces(hoverinfo='x+y', line=dict(width=2))
    fig_line.update_layout(hovermode='x')
    fig_line.show()

    # Candlestick Chart
    fig_candle = go.Figure(data=[go.Candlestick(x=df2.index,
                                                 open=df2['1. open'],
                                                 high=df2['2. high'],
                                                 low=df2['3. low'],
                                                 close=df2['4. close'])])

    fig_candle.update_layout(title='Stock Market Time Series - Candlestick Chart',
                             xaxis_title='date',
                             yaxis_title='Stock Price',
                             template='plotly_dark')
    fig_candle.update_traces(name='Candlestick', showlegend=True)

    fig_candle.show()




