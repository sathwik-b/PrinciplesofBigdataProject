import matplotlib.pyplot as plt
import matplotlib
import pandas as pd
#import xlrd
import numpy as np
import glob
f = glob.glob("q1\*.csv")
df = pd.read_csv(f[0],delimiter=',',names=['day','count'])
all_data = pd.DataFrame()
all_data = all_data.append(df,ignore_index=True)
#print(type(all_data))
labels = all_data['day']
print(labels)
sizes = all_data['count']

# Data to plot
#labels = ['Non-verified-accounts', 'Verified-Accounts']
#sizes = [204260, 1008]
colors = ['green', 'orange','yellow','red','blue','pink','violet']

# Plot
plt.pie(sizes, labels=labels, colors=colors,
        autopct='%1.1f%%', shadow=True, startangle=140)

plt.axis('equal')
plt.title('Count of users used apple in their description on that day')
plt.show()
