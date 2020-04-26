import matplotlib.pyplot as plt
import matplotlib
import pandas as pd
#import xlrd
import numpy as np
import glob
f = glob.glob("q8-1\*.csv")
df = pd.read_csv(f[0],delimiter=',',names=['hashtag','count'])
all_data = pd.DataFrame()
all_data = all_data.append(df,ignore_index=True)
#print(type(all_data))
labels = all_data['hashtag']
print(labels)
sizes = all_data['count']
colors = ['green', 'orange','yellow','blue']

# Plot
plt.pie(sizes, labels=labels, colors=colors,
        autopct='%1.1f%%', shadow=True, startangle=140)

plt.axis('equal')
plt.title('Count of keywords appeared in hashtags')
plt.show()
