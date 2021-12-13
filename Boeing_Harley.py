'''
    Author: Harley Rutherford
    Date: 13/12/2021
    Purpose: To connect to data.gov.au and collect flight data for statistical analysis.
    Outputs: Three csv files -  Question_one.csv
                                Question_two.csv
                                Question_three.csv
'''
from pandas import DataFrame as df
import pandas as pd
import requests
import os

directory = os.getcwd() #Will ensure that the CSV files are created wherever the process is run

'''
    This function is used to send a request to the API and check to make sure we get a response back
    If we get a response we take the JSON file and returns it to the main function as a dataframe object.
'''
def api_request(APIurl):
    response = requests.get(APIurl)
    if response.status_code != 200: # check to make sure we get the approriate response
        print("An error has occured, the error number is: " + str(response.status_code))
    else:
        return df.from_dict(response.json()) 



'''
    Specific to the result that was returned from data.gov
    We hope to return a clean dataframe that is easy for analysis.
    All values are strings to begin so some values need to change.
'''
def dataframe_clense(in_df):
    clean_df = df(in_df.iloc[1,2])
    clean_df['Passengers_Out'] = pd.to_numeric(clean_df['Passengers_Out'])
    clean_df['Passengers_In'] = pd.to_numeric(clean_df['Passengers_In'])
    clean_df['Freight_In_(tonnes)'] = pd.to_numeric(clean_df['Freight_In_(tonnes)'])
    clean_df['Month'] = clean_df['Month'].str[:3] #Month Simply has month
    clean_df['Date_Actual'] = pd.to_datetime(clean_df['Year'] + clean_df['Month'], format='%Y%b') #Date_Actual now has information on all dates
    clean_df.set_index('Date_Actual', inplace=True) #Set the index for the window function
    return clean_df
    


'''
    Target year is 2019, we collect all the airlines that flew that year 
    We then calculate the passengers in and passengers out for each unique group.
'''
def question_one(in_df):
    df2019 = in_df[ in_df["Year"] == "2019"]
    dfoutput = df2019.groupby(['Airline', 'Month'])[['Passengers_In','Passengers_Out']].agg('sum')
    path = directory + '/Question_one.csv'
    dfoutput.to_csv(path)



'''
    The last 6 months is interpreted as the last 6 months of recorded data.
    We group by the port country and take the total amount of passangers that arrived in that period.
    We then find which country (or countries) had the maximum value
'''
def question_two(in_df):
    df_last_six = in_df.sort_values(by="Date_Actual",ascending=True).last("6M") #last 6 months for the data collected and reported on.
    df_country = df_last_six.groupby(['Port_Country'])[['Passengers_In']].agg('sum')
    max_value = df_country["Passengers_In"].max()
    dfoutput = df_country[ df_country["Passengers_In"] == max_value]
    path = directory + '/Question_two.csv'
    dfoutput.to_csv(path)



'''
    Include the last two months of 2017 for a more accurate rolling average through 2018
    As we have indexed by the month in the cleaning stage the rolling funciton will work as a moving average over months.
'''
def question_three(in_df):
    df_years = in_df.loc['2017-11-01':'2018-12-01']
    df_airlines =  df_years.groupby(['Airline', 'Date_Actual'])[['Freight_In_(tonnes)']].agg('sum') #seems to be duplicate entries here to watch out for
    dfoutput = df_airlines.groupby(['Airline'])['Freight_In_(tonnes)'].rolling(3).mean()
    path = directory + '/Question_three.csv'
    dfoutput.to_csv(path)


'''
        The function below is the main function of the program, 
        contained is all of the individual pieces of the processes above. 
        This function will run so long as this is the main program.
'''
def main():
    main_df = api_request('https://data.gov.au/data/api/3/action/datastore_search_sql?sql=SELECT * from "809c77d8-fd68-4a2c-806f-c63d64e69842"') #We query for all information
    flights_df = dataframe_clense(main_df)
    question_one(flights_df)
    question_two(flights_df)
    question_three(flights_df)



if __name__ == "__main__":
    main()
