import sqlalchemy
import requests
import time
import json
import pandas as pd

localUsername = input("db username: ")
localPassword = input("db password: ")
localPort = "3310" # you can change this if you like. 3310 is what I used.
default_cnx = "mysql+pymysql://{}:{}@localhost:{}".format(*[localUsername, localPassword, localPort])
print(default_cnx)

folder_with_data = './data/'


# use this api resource ...
# api resource: https://covidtracking.com/data/api

class CovidProject():
    def __init__(self, **kwargs):
        self.cnxString = kwargs.get('cnxString', default_cnx)

    def run_query(self, query):
        with sqlalchemy.create_engine(self.cnxString).connect() as cnx:
            cnx.execute(query)

    def run_project(self):
        self.initialize_schema()
        self.update_data()
        self.aggregate_data()
        self.something_interesting()

    def initialize_schema(self):
        self.run_query('drop schema if exists etl_project')
        self.run_query('create schema etl_project')
        self.run_query('use etl_project') 
  
       

        ##### TODO ####
        # create necessary table and other ddl queries
        # E.G. CREATE TABLE STATE_DATA(ID INT PRIMARY_KEY, ...)
        # if you use pandas, I know you can do some of this automatically, however,
        # I would like to see you demonstrate some skills with indexing or foreign keys.

        # self.run_query('CREATE TABLE Cases( CASESID INT NOT NULL AUTO_INCREMENT, POSITIVE INT, POSITIVEINCREASE INT,NEGATIVE INT,NEGATIVEINCREASE INT, PENDING INT, TOTALTESTRESULTS INT, TOTALTESTSVIRALINCREASE INT,RECOVERED INT, POSITIVESCORE INT,TOTALTESTENCOUNTERSVIRALINCREASE INT, TOTALTESTENCOUNTERSVIRAL INT, PRIMARY KEY (CASESID))')
        
        # self.run_query('CREATE TABLE Specimens (SPECIMENSID INT NOT NULL AUTO_INCREMENT,POSITIVETESTSVIRAL INT, TOTALTESTSVIRAL INT,NEGATIVETESTSVIRAL INT, NEGATIVETESTSANTIBODY INT, POSITIVETESTSANTIBODY INT, TOTALTESTSANTIGEN INT,POSITIVETESTSANTIGEN INT, TOTALTESTSANTIBODY INT,PRIMARY KEY (SPECIMENSID);')

        # self.run_query('CREATE TABLE People (PEOPLEID INT NOT NULL AUTO_INCREMENT,TOTALTESTSPEOPLEVIRAL INT,POSITIVECASESVIRAL INT, NEGATIVETESTSPEOPLEANTIBODY INT, POSITIVETESTSPEOPLEANTIBODY INT,TOTALTESTSPEOPLEANTIGEN INT,TOTALTESTSPEOPLEVIRALINCREASE INT,TOTALTESTSPEOPLEANTIBODY INT,PRIMARY KEY (PEOPLEID)); ')

        # self.run_query('CREATE TABLE Outcomes (OUTCOMESID INT NOT NULL AUTO_INCREMENT, DEATH INT, DEATHCONFIRMED INT, DEATHINCREASE INT, DEATHPROBABLE INT, HOSPITALIZED INT, HOSPITALIZEDCUMULATIVE INT, HOSPITALIZEDCURRENTLY INT, HOSPITALIZEDINCREASE INT, INICUCUMULATIVE INT, INICUCURRENTLY INT, ONVENTILATORCUMULATIVE INT, ONVENTILATORCURRENTLY INT,PRIMARY KEY (OUTCOMESID));')

        # self.run_query('CREATE TABLE STATE_DATA (ID INT NOT NULL AUTO_INCREMENT, RECORDDATE DATE, STATE varchar(2), DATAQUALITYGRADE varchar(2),	OUTCOMESID INT,	PEOPLEID INT, SPECIMENSID INT, CASESID INT,PRIMARY KEY (ID), FOREIGN KEY (OUTCOMESID)REFERENCES OUTCOMES(OUTCOMESID), FOREIGN KEY (PEOPLEID) REFERENCES PEOPLE(PEOPLEID),FOREIGN KEY (SPECIMENSID) REFERENCES SPECIMENS(SPECIMENSID),FOREIGN KEY (CASESID) REFERENCES CASES(CASESID))')

        self.add_data_from_csv()

        print('initialized the schema')

    def add_data_from_csv(self):

        # get raw historical data from website and
        # one time upload for building tables
        # you can download from here:
        # https://covidtracking.com/data/download

        myData = pd.read_csv(folder_with_data+'all-states-history.csv')
        df = pd.DataFrame(myData)
        print(df.columns)
        # for index, row in df.iterrows():
        #     # what row am I on?
        #     print(index)

        #     cases_insert = "INSERT INTO Cases(POSITIVE = {}, POSITIVEINCREASE = {},NEGATIVE ={} ,NEGATIVEINCREASE ={} , PENDING ={} , TOTALTESTRESULTS ={} , TOTALTESTSVIRALINCREASE ={} ,RECOVERED ={}, POSITIVESCORE ={} ,TOTALTESTENCOUNTERSVIRALINCREASE ={} , TOTALTESTENCOUNTERSVIRAL ={} )".format(*[row['positive'],row['positiveIncrease'],row['negative'],row['negativeIncrease'],row['pending'],row['totalTestResults'],row['totalTestsViralIncrease'],row['recovered'],row['positiveScore'],row['totalTestEncountersViralIncrease'],row['totalTestEncountersViral']])

        #     specimens_insert = "INSERT INTO Specimens(POSITIVETESTSVIRAL = {}, TOTALTESTSVIRAL = {},NEGATIVETESTSVIRAL ={} ,NEGATIVETESTSANTIBODY ={} , POSITIVETESTSANTIBODY ={} , TOTALTESTSANTIGEN ={} , POSITIVETESTSANTIGEN ={} , TOTALTESTSANTIBODY={}  )".format(*[row['positiveTestsViral'],row['totalTestsViral'],row['negativeTestsViral'],row['negativeTestsAntibody'],row['positiveTestsAntibody'],row['totalTestsAntigen'],row['positiveTestsAntigen'],row['positiveTestsAntibody']])

        #     outcomes_insert = "INSERT INTO Outcomes(DEATH ={},DEATHCONFIRMED= {},DEATHINCREASE = {},DEATHPROBABLE ={} ,HOSPITALIZED ={} ,HOSPITALIZEDCUMULATIVE  ={} , HOSPITALIZEDCURRENTLY  ={} ,HOSPITALIZEDINCREASE ={} ,  INICUCUMULATIVE={} , INICUCURRENTLY={}, ONVENTILATORCUMULATIVE={}, ONVENTILATORCURRENTLY={} )".format(*[row['death'],row['deathConfirmed'],row['deathIncrease'],row['deathProbable'],row['hospitalized'],row['hospitalizedCumulative'],row['hospitalizedCurrently'],row['hospitalizedIncrease'],row['inIcuCumulative'],row['inIcuCurrently'],row['onVentilatorCumulative'],row['onVentilatorCurrently']])

        #     people_insert = "INSERT INTO People(TOTALTESTSPEOPLEVIRAL= {}, POSITIVECASESVIRAL = {}, NEGATIVETESTSPEOPLEANTIBODY ={} , POSITIVETESTSPEOPLEANTIBODY ={} , TOTALTESTSPEOPLEANTIGEN  ={} , TOTALTESTSPEOPLEVIRALINCREASE ={} , TOTALTESTSPEOPLEANTIBODY ={} )".format(*[row['totalTestsPeopleViral'],row['positiveCasesViral'],row['negativeTestsPeopleAntibody'],row['positiveTestsPeopleAntibody'],row['totalTestsPeopleAntigen'],row['totalTestsPeopleViralIncrease'],row['totalTestsPeopleAntibody']])
    

        #     self.run_query(cases_insert)
        #     self.run_query(specimens_insert)
        #     self.run_query(outcomes_insert)
        #     self.run_query(people_insert)
        #     self.run_query(state_data_insert)
 
    


        print("added new data")

    def update_data(self):

        start_time = time.time()

        print(requests.get('https://api.covidtracking.com/v1/states/ca/20200914.json').json())

        # figure out latest date in the database
        # once you have the date make a separate
        # query for EACH state for EACH missing date.

        # NOTE: I know you can just pull all state
        # data, but I want to see how you go about
        # combining data for states and dates
        # and inserting them in an efficient manner.

        # https://api.covidtracking.com/v1/states/ca/20200501.json

        print('data updated')
        print('took this many seconds: ', time.time() - start_time)

    def aggregate_data(self):

        ### TODO: ####
    

        # generate a table that summarizes the raw data.

        # should include...

        # 7 day rolling average for each state in deaths
        # ranking in deaths and hospitalization rates
        # for each state based on latest data

        print('aggregated data')   

    def something_interesting(self):

        # do something interesting with this data
        # e.g. combine with another data source or
        # find an interesting way of aggregating
        # it further etc.

        print('did something interesting')



if __name__ == '__main__':
    project = CovidProject()
    project.run_project()
    print("""
        How would you go about setting this up on a routine to update daily? i.e. run update_data
        briefly explain... Make a note or two for us to discuss together.
    """)
