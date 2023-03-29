# import packages
import sqlite3
import pandas as pd
import os
import matplotlib.pyplot as plt
import seaborn as sns

# Assignment: Data Analysis and Visualization (Final Submission)
# Created by Tyler Watson
# Spring 2023
# 3/9/2023
# All code is original! 

# Thanks for a great semester, Professor! :-)

# change the working directory
os.chdir(r'## hidden ##')

# connect to the SQLite database and create a cursor object
def connect_to_sqlite():
    print('Making the connection to the SQLite database...')
    conn = sqlite3.connect('CO_Labor.sqlite')
    conn.text_factory = str
    cur = conn.cursor()
    return cur, conn

# write a query to get the data from the CO_Labor table
def query_data(cur, conn):
    print('\n')
    print('Querying the data from the CO_Labor table...')
    # Initial SQL query
    query = """
    SELECT id, areaname, pertypdesc, ownership, ownertitle, estab, statename, stateabbrv, avgemp, totwage, avgwkwage, periodyear, indcode, indcodety
    FROM CO_Labor
    WHERE ownership != 0
    ORDER BY indcode, periodyear ASC
    """
    return query

# query the data and create two pandas dataframes
def create_cobiz_df(conn, query):
    # Create a pandas dataframe from the SQL query
    cobiz_df = pd.read_sql_query(query, conn)

    # group the dataframe by industry code and industry code type
    grouped_cobiz_df = cobiz_df.groupby(['indcode', 'indcodety']).agg({'estab': 'sum', 'avgemp': 'sum', 'totwage': 'sum', 'avgwkwage': 'sum'}).reset_index()
    
    return cobiz_df, grouped_cobiz_df

# print the statistics for each industry code type
def print_statistics(grouped_cobiz_df):
    # Iterate through the grouped_cobiz_df DataFrame
    for index, row in grouped_cobiz_df.iterrows():
        # Print the number of establishments in the indcodety category
        print(f"There are {row['estab']} establishments classified as a(n) {row['indcodety']} organization.")

        # Print the average number of employees of that indcodety
        print(f"-> They employ on average {row['avgemp']} employees!")

        # Print the average amount of average weekly wages of that indcodety
        print(f"--> And they have paid out {row['avgwkwage']} in average weekly wages over the last 20 years!")

        # Print the total amount of average weekly wages paid out over the last 20 years
        print(f"---> This amounts to {row['totwage']} in total wages paid over the last 20 years!")

        # Print a separator
        print('----> (wow!)')
        print('\n')

    print('The industry code types are as follows:')
    print('This is useful for interpreting the industry codes in the plots below!')
    print(grouped_cobiz_df[['indcodety', 'indcode']])
    print('\n')

# create some plots! 
def create_plots(cobiz_df):
    # Plot 1: Number of Establishments by Industry
    estab_by_industry = cobiz_df.groupby('indcodety')['estab'].sum()
    plt.bar(estab_by_industry.index, estab_by_industry.values)
    plt.xticks(rotation=90)
    plt.title('Establishments by Industry')
    plt.xlabel('Industry')
    plt.ylabel('Number of Establishments')
    plt.savefig('estab_by_industry.png')
    plt.show()

    # Plot 2: Line Chart of Total Wages by Year
    wages_by_year = cobiz_df.groupby(['periodyear'])['totwage'].sum()
    plt.plot(wages_by_year.index, wages_by_year.values) 
    plt.title('    Total Wages by Year Across All Industries Examined')
    plt.xlabel('Year')
    plt.ylabel('Total Wages')
    plt.savefig('wages_by_year.png')
    plt.show()

    # Plot 3: Stacked Bar Chart of the Total Number of Establishments by Industry and Year
    estab_by_industry_year = cobiz_df.groupby(['periodyear', 'indcode'])['estab'].sum().unstack()
    estab_by_industry_year.plot(kind='bar', stacked=True)
    plt.title('Total Number of Establishments by Industry and Year')
    plt.xlabel('Year')
    plt.ylabel('Total Number of Establishments')
    plt.savefig('estab_by_industry_year.png')
    plt.show()

    # Plot 4: Boxplot of Total Wages by Industry
    sns.boxplot(x='indcode', y='totwage', data=cobiz_df)
    plt.title('Distribution of Total Wages by Industry')
    plt.xlabel('Industry')
    plt.ylabel('Total Wages')
    plt.xticks(rotation=90)
    plt.savefig('totwage_by_industry_boxplot.png')
    plt.show()

# main function
def main():
    # Connect to the SQLite database
    cur, conn = connect_to_sqlite()

    # Query the data from the CO_Labor table
    query = query_data(cur, conn)

    # Create a pandas dataframe from the SQL query
    cobiz_df, grouped_cobiz_df = create_cobiz_df(conn, query)

    # Print some statistics
    print_statistics(grouped_cobiz_df)

    # Create some plots
    create_plots(cobiz_df)

# run the main function
if __name__ == '__main__':
    main()