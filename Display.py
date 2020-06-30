import json
import csv
import datetime
from datetime import timedelta
from time import time
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import logging

class Display():

    __instance = None

    def __init__(self, data):
        logging.info('module:{}, calling:{}, with:{}'.format('Display','__init__',data))
        self.data_list = data
        Display.__instance = self
        # atribute used in display_plot
        # list of ordered key in data_list (countries/province/combinedkey)
        self.is_master_list_generated = False
        self.data_key = []
        self.deaths_master = []
        self.confirmed_master = []
        self.recovered_master = []
        self.active_master = []
        self.dates = []
        # self.export_JSON()
        # self.export_CSV()
        # self.display_plot()

    def display_on_screen(self):
        if not self.is_master_list_generated:
            self.generate_master_list()
        
        data_key = self.get_data_key()
        print("DATA DATES:", self.dates)
        for i in range(len(self.data_key)):
            print('   '+data_key[i] + ':')
            print("Deaths:", self.deaths_master[i])
            print("Confirms:", self.confirmed_master[i])
            print("Active:", self.active_master[i])
            print("Recovered:", self.recovered_master[i])
            print('\n')
            

    
    # this is for the singleton design pattern
    def getInstance():
        if Display.__instance != None:
            return Display.__instance 
        else:
            print('no data has been query')

    def generate_data_key(self):
        logging.info('module:{}, calling:{}'.format('Display','generate_data_key'))
        for entry in self.data_list:
                key_name = ' '.join(entry[0][5:])
                self.data_key.append(key_name)

    def get_data_key(self):
        logging.info('module:{}, calling:{}'.format('Display','get_data_key'))
        if len(self.data_key) == 0:
            self.generate_data_key()
        return self.data_key

    def export_JSON(self):
        logging.info('module:{}, calling:{}'.format('Display','export_JSON'))
        data = {} 
        for entry in self.data_list:
            logging.info('module:{}, calling:{}, entry:{}'.format('Display','export_JSON',entry))
            for row in entry:
                logging.info('module:{}, calling:{}, row:{}'.format('Display','export_JSON',row))
                key_name = ' '.join(row[5:])
                entry_json = {
                        "date": row[4],
                        "deaths": row[0],
                        "confirmed": row[1],
                        "recovered": row[2],
                        "active": row[3]
                    }
                if key_name in data:
                    data[key_name].append(entry_json)
                else:
                    data[key_name] = [entry_json]
        with open('json_out.txt', 'w') as outfile:
            logging.info('module:{}, calling:{}, dumping json file'.format('Display','export_JSON'))
            json.dump(data, outfile, default=str)
            logging.info('module:{}, calling:{}, dumped'.format('Display','export_JSON'))

    def generate_master_list(self):
        # 4 plot, 
        # x-axis (number of cases)
        


        for entry in self.data_list:
            d = []
            c = []
            r = []
            a = []

            for row in entry:
                d.append(row[0])
                c.append(row[1])
                r.append(row[2])
                a.append(row[3])

                #append date
                if len(self.dates) < len(entry):
                    self.dates.append(datetime.datetime.date(row[4]))
            self.deaths_master.append(d)
            self.confirmed_master.append(c)
            self.recovered_master.append(r)
            self.active_master.append(a)

    def plot(self,name):

        

        if not self.is_master_list_generated:
            self.generate_master_list()

        if name == 'D':
            master_list = self.deaths_master
            name = 'Death'
        if name == 'R':
            master_list = self.recovered_master
            name = 'Recovered'
        if name == 'A':
            master_list = self.active_master
            name = 'Active'
        if name == 'C':
            master_list = self.confirmed_master
            name = 'Confirmed'

        fig, plot = plt.subplots()
        plt.xticks(np.arange(self.dates[0], self.dates[-1]+datetime.timedelta(days=1)))
        plt.gcf().autofmt_xdate()
        data_key = self.get_data_key()
        
   
        i = 0
        
        while i < len(data_key):
            plot.scatter(self.dates, master_list[i], label = data_key[i])
            for x,y in zip(self.dates, master_list[i]):

                label = "{:.2f}".format(y)

                # this method is called for each point
                plt.annotate(label, # this is the text
                            (x,y), # this is the point to label
                            textcoords="offset points", # how to position the text
                            xytext=(0,10), # distance from text to points (x,y)
                            ha='right') # horizontal alignment can be left, right or center
            i += 1
        
        
        
        myFmt = mdates.DateFormatter('%Y-%m-%d')
        plot.xaxis.set_major_formatter(myFmt)

        plot.set_ylabel('number of cases')  # Add an x-label to the axes.
        plot.set_xlabel('date')  # Add a y-label to the axes.
        plot.set_title(name)  # Add a title to the axes.
        plot.fmt_xdata = mdates.DateFormatter('%Y-%m-%d')
        plot.legend()  # Add a legend.

        plt.show()
        
                        

                
        






    def export_CSV(self):
        with open('csv_out.csv','w') as out:
            csv_out=csv.writer(out)
            csv_out.writerow(['deaths','confirmed','recovered','active','date','name'])
            for entry in self.data_list:
                for row in entry:
                    csv_out.writerow(row)

if __name__ == '__main__':
    data = [[(621.0, 21533.0, 0.0, 212.0, datetime.datetime(2020, 6, 18, 0, 0), 'South Carolina')],[(6211.0, 21533.0, 0.0, 2912.0, datetime.datetime(2020, 6, 18, 0, 0), 'China')],[(61.0, 21533.0, 0.0, 20912.0, datetime.datetime(2020, 6, 18, 0, 0), 'Vietnam')]]
    D = Display(data)
    D.export_JSON()
    D.plot('A')





