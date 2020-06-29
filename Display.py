import json
import csv
import datetime
from datetime import timedelta
from time import time
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np

class Display():

    def __init__(self, data):
        self.data_list = data
        # atribute used in display_plot
        # list of ordered key in data_list (countries/province/combinedkey)
        self.data_key = []
        # self.export_JSON()
        # self.export_CSV()
        # self.display_plot()

    def generate_data_key(self):
        for entry in self.data_list:
                key_name = ' '.join(entry[0][5:])
                self.data_key.append(key_name)

    def get_data_key(self):
        if len(self.data_key) == 0:
            self.generate_data_key()
        return self.data_key

    def export_JSON(self):
        data = {} 
        for entry in self.data_list:
            for row in entry:
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
            json.dump(data, outfile, default=str)

    def display_plot(self):
        # 4 plot, 
        # x-axis (number of cases)
        deaths_master = []
        confirmed_master = []
        recovered_master = []
        active_master = []

        # loop for y-axis (date)
        xx = []


        for entry in self.data_list:
            print(entry)
            d = []
            c = []
            r = []
            a = []

            for row in entry:
                print('row:', row)
                d.append(row[0])
                c.append(row[1])
                r.append(row[2])
                a.append(row[3])

                #append date
                if len(xx) < len(entry):
                    xx.append(datetime.datetime.date(row[4]))
                    print(xx)
            deaths_master.append(d)
            confirmed_master.append(c)
            recovered_master.append(r)
            active_master.append(a)


        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2,2)
        # fig, dp = plt.subplots()  # a figure with a single Axes
        plt.xticks(np.arange(xx[0], xx[-1]+datetime.timedelta(days=1)))
        plt.gcf().autofmt_xdate()

        self.plot_helper(ax1,deaths_master,xx)
        self.plot_helper(ax2,confirmed_master,xx)
        self.plot_helper(ax3,recovered_master,xx)
        self.plot_helper(ax4,active_master,xx)

        plt.show()


    def plot_helper(self, plot, master_list,xx):
        i = 0
        #create deaths plot
        while i < len(self.data_key):
            print(xx)
            plot.scatter(xx, master_list[i], label = self.data_key[i])
            i += 1
        # dp.scatter( xx, deaths_master[0], label = data_key[0])
        # dp.scatter( xx, deaths_master[1], label = data_key[1])
        
        
        myFmt = mdates.DateFormatter('%Y-%m-%d')
        plot.xaxis.set_major_formatter(myFmt)

        plot.set_ylabel('number of cases')  # Add an x-label to the axes.
        plot.set_xlabel('date')  # Add a y-label to the axes.
        plot.set_title("DEATHS ")  # Add a title to the axes.
        plot.fmt_xdata = mdates.DateFormatter('%Y-%m-%d')
        plot.legend()  # Add a legend.
        
                        

                
        






    # def export_CSV(self):
    #     myFile = open('CSV_out.csv', 'w')
    #     with myFile:python -m pip install -U pip
    #         writer = csv.writer(myFile)
    #         for key in self.data_list:
    #             writer.writerows(self.data_list)

if __name__ == '__main__':
    D = Display()
    D.generate_data_key()





#[(621.0, 21533.0, 0.0, 20912.0, datetime.datetime(2020, 6, 18, 0, 0), 'South Carolina')]