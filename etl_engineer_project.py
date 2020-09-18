import sqlalchemy
import requests
import time
import json
import pandas as pd
import datetime
from datetime import date, timedelta
import matplotlib.pyplot as plt
import numpy as np


# from api_assistant import apiAssistant


localUsername = input("db username: ")
localPassword = input("db password: ")
localPort = "3310"  # you can change this if you like. 3310 is what I used.
default_cnx = "mysql+pymysql://{}:{}@localhost:{}".format(
    *[localUsername, localPassword, localPort])
new_cnx = "mysql+pymysql://{}:{}@localhost:{}/etl_project".format(
    *[localUsername, localPassword, localPort])
print(default_cnx)


folder_with_data = './data/'


# use this api resource ...
# api resource: https://covidtracking.com/data/api

class CovidProject():
    def __init__(self, **kwargs):
        self.cnxString = kwargs.get('cnxString', default_cnx)
        self.cnxString2 = kwargs.get('cnxString2', new_cnx)
        with sqlalchemy.create_engine(self.cnxString).connect() as cnx:
            cnx.execute('drop schema if exists etl_project')
            cnx.execute('create schema etl_project')

    def run_query(self, query):
        with sqlalchemy.create_engine(self.cnxString2).connect() as cnx:
            cnx.execute(query)

    def run_project(self):
        self.initialize_schema()
        self.update_data()
        self.aggregate_data()
        self.something_interesting()

    def initialize_schema(self):

        ##### TODO ####
        # create necessary table and other ddl queries
        # E.G. CREATE TABLE STATE_DATA(ID INT PRIMARY_KEY, ...)
        # if you use pandas, I know you can do some of this automatically, however,
        # I would like to see you demonstrate some skills with indexing or foreign keys.

        # Creating cases table
        self.run_query('CREATE TABLE Cases( CASESID INT NOT NULL AUTO_INCREMENT, POSITIVE INT, POSITIVEINCREASE INT,NEGATIVE INT,NEGATIVEINCREASE INT, PENDING INT, TOTALTESTRESULTS INT, TOTALTESTSVIRALINCREASE INT,RECOVERED INT, POSITIVESCORE INT,TOTALTESTENCOUNTERSVIRALINCREASE INT, TOTALTESTENCOUNTERSVIRAL INT, POSITIVETESTSVIRAL INT, TOTALTESTSVIRAL INT,NEGATIVETESTSVIRAL INT, NEGATIVETESTSANTIBODY INT, POSITIVETESTSANTIBODY INT, TOTALTESTSANTIGEN INT,POSITIVETESTSANTIGEN INT, TOTALTESTSANTIBODY INT, TOTALTESTSPEOPLEVIRAL INT,POSITIVECASESVIRAL INT, NEGATIVETESTSPEOPLEANTIBODY INT, POSITIVETESTSPEOPLEANTIBODY INT,TOTALTESTSPEOPLEANTIGEN INT,TOTALTESTSPEOPLEVIRALINCREASE INT,TOTALTESTSPEOPLEANTIBODY INT, PRIMARY KEY (CASESID))')

        # Creating STATE_DATA table
        self.run_query('CREATE TABLE STATE_DATA (ID INT NOT NULL AUTO_INCREMENT, RECORDDATE DATE, STATENAME char(2), DATAQUALITYGRADE varchar(4),	OUTCOMESID INT,	PEOPLEID INT, SPECIMENSID INT, CASESID INT,PRIMARY KEY (ID), DEATH INT, DEATHCONFIRMED INT, DEATHINCREASE INT, DEATHPROBABLE INT, HOSPITALIZED INT, HOSPITALIZEDCUMULATIVE INT, HOSPITALIZEDCURRENTLY INT, HOSPITALIZEDINCREASE INT, INICUCUMULATIVE INT, INICUCURRENTLY INT, ONVENTILATORCUMULATIVE INT, ONVENTILATORCURRENTLY INT, FOREIGN KEY (CASESID) REFERENCES CASES(CASESID))')

        self.add_data_from_csv()

        print('initialized the schema')

    def add_data_from_csv(self):

        # get raw historical data from website and
        # one time upload for building tables
        # you can download from here:
        # https://covidtracking.com/data/download

        # bring out the data
        myData = pd.read_csv(
            folder_with_data+'all-states-history.csv', na_filter=False)

        df = pd.DataFrame(myData)
        df2 = df.replace('', 'NULL')
        # print(df2.columns)

        # building my cases query
        cases_insert_first_half = 'INSERT INTO Cases(POSITIVE , POSITIVEINCREASE ,NEGATIVE ,NEGATIVEINCREASE  , PENDING  , TOTALTESTRESULTS  , TOTALTESTSVIRALINCREASE  ,RECOVERED , POSITIVESCORE ,TOTALTESTENCOUNTERSVIRALINCREASE  , TOTALTESTENCOUNTERSVIRAL, POSITIVETESTSVIRAL, TOTALTESTSVIRAL,NEGATIVETESTSVIRAL ,NEGATIVETESTSANTIBODY  , POSITIVETESTSANTIBODY  , TOTALTESTSANTIGEN  , POSITIVETESTSANTIGEN  , TOTALTESTSANTIBODY,TOTALTESTSPEOPLEVIRAL, POSITIVECASESVIRAL, NEGATIVETESTSPEOPLEANTIBODY  , POSITIVETESTSPEOPLEANTIBODY  , TOTALTESTSPEOPLEANTIGEN   , TOTALTESTSPEOPLEVIRALINCREASE  , TOTALTESTSPEOPLEANTIBODY ) values'
        cases_insert_values = []

        # building my state query
        state_insert_first_half = 'INSERT INTO STATE_DATA(RECORDDATE, STATENAME, DATAQUALITYGRADE, DEATH,DEATHCONFIRMED,DEATHINCREASE,DEATHPROBABLE  ,HOSPITALIZED, HOSPITALIZEDCUMULATIVE, HOSPITALIZEDCURRENTLY, HOSPITALIZEDINCREASE,  INICUCUMULATIVE, INICUCURRENTLY, ONVENTILATORCUMULATIVE, ONVENTILATORCURRENTLY, CASESID ) VALUES '
        state_insert_values = []

        # iterate over the rows, append each new section above
        for index, row in df2.iterrows():

            # building one section of my cases query
            case_row_data = '({},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}),'.format(*[row['positive'], row['positiveIncrease'], row['negative'], row['negativeIncrease'], row['pending'], row['totalTestResults'], row['totalTestsViralIncrease'], row['recovered'], row['positiveScore'], row['totalTestEncountersViralIncrease'], row['totalTestEncountersViral'], row['positiveTestsViral'], row[
                'totalTestsViral'], row['negativeTestsViral'], row['negativeTestsAntibody'], row['positiveTestsAntibody'], row['totalTestsAntigen'], row['positiveTestsAntigen'], row['positiveTestsAntibody'], row['totalTestsPeopleViral'], row['positiveCasesViral'], row['negativeTestsPeopleAntibody'], row['positiveTestsPeopleAntibody'], row['totalTestsPeopleAntigen'], row['totalTestsPeopleViralIncrease'], row['totalTestsPeopleAntibody']])
           

            # building one section of my state query
            state_row_data = '(\'{}\',\'{}\',\'{}\',{},{},{},{},{},{},{},{},{},{},{},{},{}),'.format(*[row['date'], row['state'], row['dataQualityGrade'], row['death'], row['deathConfirmed'], row['deathIncrease'], row['deathProbable'], row[
                'hospitalized'], row['hospitalizedCumulative'], row['hospitalizedCurrently'], row['hospitalizedIncrease'], row['inIcuCumulative'], row['inIcuCurrently'], row['onVentilatorCumulative'], row['onVentilatorCurrently'], index+1])
          

            # append my new sections
            cases_insert_values.append(case_row_data)
            state_insert_values.append(state_row_data)

        # preparing the query, putting the parts together
        for x in range(len(state_insert_values)):
            state_insert_first_half = state_insert_first_half + state_insert_values[x]
            cases_insert_first_half = cases_insert_first_half + cases_insert_values[x]

       
        cases_insert_complete = cases_insert_first_half[:-1]
        state_insert_complete = state_insert_first_half[:-1]

        self.run_query(cases_insert_complete)
        self.run_query(state_insert_complete)
       
        print("added new data")

    def update_data(self):
        # figure out latest date in the database
        # once you have the date make a separate
        # query for EACH state for EACH missing date.

        # NOTE: I know you can just pull all state
        # data, but I want to see how you go about
        # combining data for states and dates
        # and inserting them in an efficient manner.

        # https://api.covidtracking.com/v1/states/ca/20200501.json
        start_time = time.time()
        db = sqlalchemy.create_engine(self.cnxString2).connect()

        # print(requests.get('https://api.covidtracking.com/v1/states/ca/20200914.json').json())

        latest_date = db.execute(
            'SELECT RECORDDATE FROM STATE_DATA ORDER BY RECORDDATE DESC LIMIT 1'
        ).fetchall()

        latest_date_df = pd.DataFrame(latest_date)
        latest_date_df.columns = latest_date[0].keys()
        my_date = str(latest_date_df['RECORDDATE'][0])
        print('My Latest Date ' + my_date)

        # find current date unix and readable

        current_date_unix = int(time.time())
        current_date_readable = datetime.datetime.fromtimestamp(
            current_date_unix).strftime('%Y-%m-%d')
        print('CURRENT TIME AND DAY')
        print(current_date_readable)

        # pass them through the date()
        db_date = date(int(my_date[0:4]), int(
            my_date[5:7]), int(my_date[8:10]))
        current_date = date(int(current_date_readable[0:4]), int(
            current_date_readable[5:7]), int(current_date_readable[8:10]))
  

        # make a list of the needed days
        my_dates_datetime = pd.date_range(
            db_date, current_date-timedelta(days=1), freq='d')
        my_dates_list = my_dates_datetime.format('%Y-%m-%d')
      
        print('Missing Dates List:')
        # a blank record is in there
        for x in my_dates_list:
            if x == '':
                my_dates_list.remove(x)
        print(my_dates_list)

        # get latest id
        db2 = sqlalchemy.create_engine(self.cnxString2).connect()
        select_ID = 'select ID, CASES.CASESID from STATE_DATA, CASES where STATE_DATA.ID = CASES.CASESID ORDER BY ID DESC LIMIT 1'
        my_ids = db2.execute(select_ID)
        id_df = pd.DataFrame(my_ids)
        print('LATEST ID')

        latest_id = id_df[0][0] + 1
        print(latest_id)

        URLS = []

        STATES = ['ak', 'al', 'ar', 'as', 'az', 'ca', 'co', 'ct', 'dc', 'de', 'fl', 'ga', 'gu', 'hi', 'ia', 'id', 'il', 'in', 'ks', 'ky', 'la', 'ma', 'md', 'me', 'mi', 'mn', 'mo',
                  'mp', 'ms', 'mt', 'nc', 'nd', 'ne', 'nh', 'nj', 'nm', 'nv', 'ny', 'oh', 'ok', 'or', 'pr', 'ri', 'sc', 'sd', 'tn', 'tx', 'ut', 'va', 'vi', 'vt', 'wa', 'wi', 'wv', 'wy', 'pa']

        DATES = ['20200915', '20200916']

        df_master = pd.DataFrame(columns=['RECORDDATE', 'STATENAME', 'DATAQUALITYGRADE', 'DEATH', 'DEATHCONFIRMED', 'DEATHINCREASE', 'DEATHPROBABLE', 'HOSPITALIZED', 'HOSPITALIZEDCUMULATIVE', 'HOSPITALIZEDCURRENTLY', 'HOSPITALIZEDINCREASE',  'INICUCUMULATIVE', 'INICUCURRENTLY', 'ONVENTILATORCUMULATIVE', 'ONVENTILATORCURRENTLY', 'POSITIVE', 'POSITIVEINCREASE', 'NEGATIVE', 'NEGATIVEINCREASE', 'PENDING', 'TOTALTESTRESULTS', 'RECOVERED', 'POSITIVESCORE', 'TOTALTESTENCOUNTERSVIRAL', 'POSITIVETESTSVIRAL', 'TOTALTESTSVIRAL', 'NEGATIVETESTSVIRAL', 'NEGATIVETESTSANTIBODY', 'POSITIVETESTSANTIBODY', 'TOTALTESTSANTIGEN', 'POSITIVETESTSANTIGEN', 'TOTALTESTSANTIBODY', 'TOTALTESTSPEOPLEVIRAL', 'POSITIVECASESVIRAL', 'NEGATIVETESTSPEOPLEANTIBODY', 'POSITIVETESTSPEOPLEANTIBODY', 'TOTALTESTSPEOPLEANTIGEN', 'TOTALTESTSPEOPLEANTIBODY'])

        url_start = 'https://api.covidtracking.com/v1/states/'

        url_end = '.json'
        for x in STATES:
            for each_date in DATES:
                complete_url = url_start + x + '/' + each_date + url_end
                URLS.append(complete_url)

        for x in URLS:
            response = requests.get(x)
            if response.status_code == 200:

                data = response.json()
                row_to_add = [data['date'], data['state'], data['dataQualityGrade'], data['death'], data['deathConfirmed'], data['deathIncrease'], data['deathProbable'], data['hospitalized'], data['hospitalizedCumulative'], data['hospitalizedCurrently'], data['hospitalizedIncrease'], data['inIcuCumulative'], data['inIcuCurrently'], data['onVentilatorCumulative'], data['onVentilatorCurrently'], data['positive'], data['positiveIncrease'], data['negative'], data['negativeIncrease'], data['pending'], data['totalTestResults'],
                              data['recovered'], data['positiveScore'], data['totalTestEncountersViral'], data['positiveTestsViral'], data['totalTestsViral'], data['negativeTestsViral'], data['negativeTestsAntibody'], data['positiveTestsAntibody'], data['totalTestsAntigen'], data['positiveTestsAntigen'], data['totalTestsAntibody'], data['totalTestsPeopleViral'], data['positiveCasesViral'], data['negativeTestsPeopleAntibody'], data['positiveTestsPeopleAntibody'], data['totalTestsPeopleAntigen'], data['totalTestsPeopleAntibody']]

                df_master = df_master.append(pd.DataFrame(
                    [row_to_add], columns=df_master.columns))

        df_master = df_master.fillna('NULL')
        print(df_master)
        # now to build my SQL queries
        cases_insert_first_half = 'insert into Cases(POSITIVE, POSITIVEINCREASE, NEGATIVE, NEGATIVEINCREASE, PENDING, TOTALTESTRESULTS, RECOVERED, POSITIVESCORE, TOTALTESTENCOUNTERSVIRAL, POSITIVETESTSVIRAL, TOTALTESTSVIRAL, NEGATIVETESTSVIRAL, NEGATIVETESTSANTIBODY, POSITIVETESTSANTIBODY, TOTALTESTSANTIGEN, POSITIVETESTSANTIGEN, TOTALTESTSANTIBODY, TOTALTESTSPEOPLEVIRAL, POSITIVECASESVIRAL, NEGATIVETESTSPEOPLEANTIBODY, POSITIVETESTSPEOPLEANTIBODY, TOTALTESTSPEOPLEANTIGEN, TOTALTESTSPEOPLEANTIBODY ) values'
        cases_insert_values = []

        # building my state query
        state_insert_first_half = 'insert into STATE_DATA(RECORDDATE, STATENAME, DATAQUALITYGRADE, DEATH,DEATHCONFIRMED,DEATHINCREASE,DEATHPROBABLE, HOSPITALIZED, HOSPITALIZEDCUMULATIVE, HOSPITALIZEDCURRENTLY, HOSPITALIZEDINCREASE,  INICUCUMULATIVE, INICUCURRENTLY, ONVENTILATORCUMULATIVE, ONVENTILATORCURRENTLY, CASESID ) values'
        state_insert_values = []

        # iterate over the rows, append each new section above
        for index, row in df_master.iterrows():

            # building one section of my cases query
            case_row_data = '({},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}),'.format(*[row['POSITIVE'], row['POSITIVEINCREASE'], row['NEGATIVE'], row['NEGATIVEINCREASE'], row['PENDING'], row['TOTALTESTRESULTS'], row['RECOVERED'], row['POSITIVESCORE'], row['TOTALTESTENCOUNTERSVIRAL'], row['POSITIVETESTSVIRAL'], row['TOTALTESTSVIRAL'], row[
                'NEGATIVETESTSVIRAL'], row['NEGATIVETESTSANTIBODY'], row['POSITIVETESTSANTIBODY'], row['TOTALTESTSANTIGEN'], row['POSITIVETESTSANTIGEN'], row['TOTALTESTSANTIBODY'], row['TOTALTESTSPEOPLEVIRAL'], row['POSITIVECASESVIRAL'], row['NEGATIVETESTSPEOPLEANTIBODY'], row['POSITIVETESTSPEOPLEANTIBODY'], row['TOTALTESTSPEOPLEANTIGEN'], row['TOTALTESTSPEOPLEANTIBODY']])
            # print('CASES DONE')

            # building one section of my state query
            state_row_data = '(\'{}\',\'{}\',\'{}\',{},{},{},{},{},{},{},{},{},{},{},{},{}),'.format(*[row['RECORDDATE'], row['STATENAME'], row['DATAQUALITYGRADE'], row['DEATH'], row['DEATHCONFIRMED'], row['DEATHINCREASE'], row['DEATHPROBABLE'], row[
                'HOSPITALIZED'], row['HOSPITALIZEDCUMULATIVE'], row['HOSPITALIZEDCURRENTLY'], row['HOSPITALIZEDINCREASE'], row['INICUCUMULATIVE'], row['INICUCURRENTLY'], row['ONVENTILATORCUMULATIVE'], row['ONVENTILATORCURRENTLY'], index+latest_id])
            # print('State DONE')

            # append my new sections

            cases_insert_values.append(case_row_data)
            state_insert_values.append(state_row_data)

        # preparing the query, putting the parts together
        for x in range(len(state_insert_values)):
            state_insert_first_half = state_insert_first_half + state_insert_values[x]
            cases_insert_first_half = cases_insert_first_half + cases_insert_values[x]
   
       
            

        # completed queries, very long
        cases_insert_complete = cases_insert_first_half[:-1]
        state_insert_complete = state_insert_first_half[:-1]

        # run the queries

        self.run_query(cases_insert_complete)
        self.run_query(state_insert_complete)

        print('data updated')
        print('took this many seconds: ', time.time() - start_time)

    def aggregate_data(self):

        ### TODO: ####

        # generate a table that summarizes the raw data.

        # should include...

        # 7 day rolling average for each state in deaths
        # ranking in deaths and hospitalization rates
        # for each state based on latest data

        # two connection strings to avoid "table definition has changed". I am sure there is a better way.
        db = sqlalchemy.create_engine(self.cnxString2).connect()
        db2 = sqlalchemy.create_engine(self.cnxString2).connect()

        # initial query to grab the data for the last seven days
        seven_day_query = 'SELECT RECORDDATE, statename, hospitalized, avg(death) as \'Average Death\' from STATE_DATA where date(RECORDDATE) >= CURDATE() - interval 7 day group by statename'

       # put it into a dataframe
        result = db.execute(seven_day_query).fetchall()
        df = pd.DataFrame(result)
        df2 = df.fillna('NULL')
        df2.columns = result[0].keys()

        # Create statement
        create_aggregated_table = 'CREATE TABLE ROLLINGAVERAGE (ID INT NOT NULL AUTO_INCREMENT, STATENAME char(2), HOSPITALIZED INT, AVGDEATH INT, PRIMARY KEY (ID))'

        # preparing the query
        aggregate_insert_first_half = 'INSERT INTO ROLLINGAVERAGE(STATENAME,HOSPITALIZED,AVGDEATH) values '
        aggregate_insert_values = []

        # iterate over all rows and append the parts above
        for index, row in df2.iterrows():
            # print(index)
            aggregate_row_data = '(\'{}\',{},{}),'.format(
                *[row['statename'], row['hospitalized'], row['Average Death']])
            aggregate_insert_values.append(aggregate_row_data)

        # put the pieces together
        for x in aggregate_insert_values:
            aggregate_insert_first_half = aggregate_insert_first_half + x

        # trim the trailing comma
        aggregate_insert_complete = aggregate_insert_first_half[:-1]

        # run my commands
        self.run_query(create_aggregated_table)
        self.run_query(aggregate_insert_complete)

        # show the table in the console
        result_rolling = db2.execute('select * from ROLLINGAVERAGE').fetchall()
        rolling_df = pd.DataFrame(result_rolling)
        rolling_df.columns = result_rolling[0].keys()
        print(rolling_df)

        print('aggregated data')

    def something_interesting(self):
        # pull data and place in dataframe
        db = sqlalchemy.create_engine(self.cnxString2).connect()
        result = db.execute(
            'select statename, AVGDEATH from rollingaverage').fetchall()
        df = pd.DataFrame(result)
        df.columns = result[0].keys()

        # plot a simple bar chart, with death on the Y-axis and states on the X-axis
        states = df['statename']
        y_pos = np.arange(len(states))
        deaths = df['AVGDEATH']
        plt.figure(figsize=(30, 10))
        plt.bar(y_pos, deaths, align='center', alpha=0.5)
        plt.xticks(y_pos, states)
        plt.ylabel('Deaths')
        plt.title('7 day rolling average of deaths by state')

        plt.show()
        print('did something interesting')


if __name__ == '__main__':
    project = CovidProject()
    project.run_project()
    print("""
        How would you go about setting this up on a routine to update daily? i.e. run update_data
        briefly explain... Make a note or two for us to discuss together.

        First we would need to add persistence to the program. Our current application deletes 
        itself and would need to make a larger number of api calls each day that passes. One way 
        we can accomplish persistence is by ridding ourselves of the drop/create statements for the schema.
        We would also need to only add the initial csv file one time and never do it again. Second we would 
        need to grab data on an interval. In this case, daily. We could set up a cron job or something equivalent 
        to have our app run that code when we need it to. Our app would need to always be running. To accomlish this, 
        we could host it on heroku or a linux vps. If needed, we may want a process management tool like PM-2.

        Another way to add some persistence is to call the api for the current csv everytime instead of using the downloaded file. 

        If I started over, I would just have one table. I didnt see a good way to normalize this database. In retrospect, 
        I basically split it arbitrarily. The pd.Dataframe().to_sql() method is way better than what I have and my setup made 
        it hard to deal with. I would also successfully tackle an asynchronous api call setup. I tried threading, asyncio, and  
        multiprocessing. All three would mostly work, and were definitely faster. Eventually, I ran into too many errors with it
        that were eating up alot of time. I made the executive decision to keep my low-quality two table setup and to continue constructing
        bulk queries with finnicky manual foreign key assignments. A stricter focus from the start on pandas, my choices of tables, and 
        asynchronous code would have made this alot easier from the beginning. 
    
        I constructed bulk queries by iterating over the rows. After building the string I passed it through the run_query method. It was fast. 
        I learned alot throughout this project about speed and efficiency. This was alot of fun and I am even more convinced that this field is my interest
        as a programmer. 

    """)
