import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import csv

x_values = []
w_values = []
li_values = []

with open("demo.csv", 'w') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['Time', 'Workflows', 'Live Instances'])

fig, (ax1, ax2) = plt.subplots(2,1)
def animate(self):
    data = pd.read_csv('demo.csv')
    x_values = data['Time']
    w_values = data['Workflows']
    li_values = data['Live Instances']
    ax1.cla()
    ax1.plot(x_values, w_values)
    #ax1.xlabel('Time')
    ax1.grid()
    ax1.legend(['Workflows',], loc='upper left')
    ax2.cla()
    ax2.plot(x_values, li_values, color='red')
    #ax2.xlabel('Time')
    ax2.grid()
    ax2.legend(['Live Instances'], loc='upper left')
    plt.xlabel('Time (s)')

ani = FuncAnimation(plt.gcf(), animate, 5000)

plt.tight_layout()
plt.show()
