# importing packages
import pandas as pd
import psycopg2
import matplotlib.pyplot as plt
import pandas.io.sql as sqlio
import seaborn as sns
from sqlalchemy import create_engine, event, text, exc
from statsmodels.tsa.seasonal import STL
import plotly.graph_objects as go
from sqlalchemy.engine.url import URL
from dagster import asset
@asset
def script1():
    # reading csv file
    csv_file_path = 'dataset\\ADANIPORTS.csv'
    data = pd.read_csv(csv_file_path)
    print(data.isnull().sum())
    # Changing the date format in the dataset to yyyy-mm-dd
    data['Date'] = pd.to_datetime(data['Date'], format='%Y-%m-%d')
    # checking the version
    connection_string = "postgresql+psycopg2://dap:dap@localhost:5432/dapproject"

    try:
        engine = create_engine(connection_string)
        with engine.connect() as connection:
            server_version = sqlio.read_sql_query(text("SELECT VERSION();"), connection)
            print("Server Version:", server_version["version"].values[0])

    except exc.SQLAlchemyError as dbError:
        print("PostgreSQL Error:", dbError)

    finally:
        if 'engine' in locals():
            engine.dispose()
    # creating database and Defining the PostgreSQL connection parameters
    db_url = "postgresql+psycopg2://dap:dap@localhost:5432/dapproject"

    try:
        engine = create_engine(db_url)
        with engine.connect():
            pass

        print("Database 'dapproject' already exists.")

    except exc.SQLAlchemyError as dbError:
        print("PostgreSQL Error:", dbError)

    finally:
        if 'engine' in locals():
            engine.dispose()
    #creating engine
    engine = create_engine(db_url)

    # Defining the table schema
    table_schema = """
    CREATE TABLE IF NOT EXISTS adanidata (
        Date date,
        Symbol varchar,
        Series varchar,
        "Prev Close" numeric,
        Open numeric,
        High numeric,
        Low numeric,
        Last numeric,
        Close numeric,
        VWAP numeric,
        Volume bigint,
        Turnover numeric,
        Trades numeric,
        "Deliverable Volume" bigint,
        "%Deliverble" numeric
    );
    """

    # Executing the CREATE TABLE statement
    with engine.connect() as connection:
        connection.execute(text(table_schema))

    print("Table created successfully.")

    csv_file_path = "dataset\\ADANIPORTS.csv"
    df = pd.read_csv(csv_file_path)

    table_name = "adanidata"

    # Loading the DataFrame into the PostgreSQL database
    df.to_sql(table_name, engine, if_exists="replace", index=False)

    print(f"Data successfully loaded into the PostgreSQL table: {table_name}")

    # giving the connecting the string
    database_url = "postgresql://dap:dap@127.0.0.1:5432/dapproject"


    def create_connection():
        return psycopg2.connect(database_url)

    #Retrieving data from postgresSQL database
    def retrieve_data():
        connection = create_connection()

        try:
            with connection.cursor() as cursor:

                table_name = 'adanidata'

                query = f"SELECT * FROM {table_name}"

                cursor.execute(query)
                rows = cursor.fetchall()
                for row in rows:
                    print(row)

        except Exception as e:
            print(f"Error: {e}")

        finally:
            if connection:
                connection.close()

    if __name__ == "__main__":
        retrieve_data()
    #Exploratory data analysis
    #checking the null values
    print(df.isnull().sum())
    # Impute missing values with the mean of the 'Trades' column
    mean_trades = df['Trades'].mean()
    df['Trades'].fillna(mean_trades, inplace=True)
    # Verify if missing values are filled
    print(df.isnull().sum())

    print(df.head())

    # Summary statistics of the numerical columns
    print("\nSummary statistics:")
    print(df.describe())

    print("\nDataset information:")
    print(df.info())
    # Select columns of object type
    object_columns = df.select_dtypes(include=['object']).columns

    # Check unique values for each object-type column
    for column in object_columns:
        unique_values = df[column].unique()
        print(f"Unique values for '{column}': {unique_values}")
        print(f"Number of unique values: {len(unique_values)}\n")
    # Series column EQ is not useful, hence dropped the column
    df.drop('Series', axis=1,inplace=True)
    print(df.columns)
    #encoding the categorical variable 'symbol'
    df1 = pd.get_dummies(df, columns=['Symbol'])
    print(df1.head())
    # Convert non-numeric columns to numeric (excluding object types)
    df_numeric = df.select_dtypes(exclude=['object'])
    # Create a correlation matrix for numeric columns
    correlation_matrix = df_numeric.corr()
    # Plot the heatmap
    plt.figure(figsize=(10, 8))
    sns.heatmap(correlation_matrix, annot=True, cmap="mako", fmt=".2f")
    plt.title("Correlation Matrix")
    plt.show()
    # Convert 'Date' column to datetime
    df1['Date'] = pd.to_datetime(df1['Date'])

    # Set 'Date' as the index
    df1.set_index('Date', inplace=True)

    # Convert columns to numeric, excluding object types
    df1_numeric = df1.apply(pd.to_numeric, errors='coerce')

    # Drop columns that couldn't be converted to numeric
    df1_numeric = df1_numeric.dropna(axis=1, how='all')

    # Resample to monthly frequency
    df2 = df1_numeric.resample('M').mean()

    # Print the resulting DataFrame
    print("Daily to Monthly (date as index):")
    print(df2)

    # Time series decomposition for column 'high' to check trend,sesonality and residual
    result = STL(df2['High']).fit()
    trend_component = result.trend
    seasonal_component = result.seasonal
    residual_component = result.resid

    # Plot the components
    plt.figure(figsize=(12, 8))

    plt.subplot(4, 1, 1)
    plt.plot(trend_component, label='trend',color='blue')
    plt.title('Trend Component')
    plt.legend()

    plt.subplot(4, 1, 2)
    plt.plot(seasonal_component, label='seasonal',color='green')
    plt.title('Seasonal Component')
    plt.legend()

    plt.subplot(4, 1, 3)
    plt.plot(residual_component, label='residual',color='orange')
    plt.title('Residual Component')
    plt.legend()

    plt.subplot(4, 1, 4)
    plt.plot(df2['High'], label='Original Data', color='black')
    plt.title('Original Data')
    plt.legend()

    plt.tight_layout()
    plt.show()
    # Line Chart for column 'high'
    fig_line = go.Figure()
    fig_line.add_trace(go.Scatter(x=df2.index, y=df2['High'], mode='lines', name='High Price'))
    fig_line.update_layout(title='Stock Market Time Series - High Price',
                          xaxis_title='date',
                          yaxis_title='High Price',
                          template='plotly_dark')
    fig_line.update_traces(hoverinfo='x+y', line=dict(width=2))
    fig_line.update_layout(hovermode='x')
    fig_line.show()

    # Candlestick Chart
    fig_candle = go.Figure(data=[go.Candlestick(x=df2.index,
                                                 open=df2['Open'],
                                                 high=df2['High'],
                                                 low=df2['Low'],
                                                 close=df2['Close'])])

    fig_candle.update_layout(title='Stock Market Time Series - Candlestick Chart',
                             xaxis_title='date',
                             yaxis_title='Stock Price',
                             template='plotly_dark')
    fig_candle.update_traces(name='Candlestick', showlegend=True)
    fig_candle.show()



