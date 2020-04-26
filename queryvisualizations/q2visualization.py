import matplotlib.pyplot as plt
import matplotlib
import pandas as pd
import numpy as np
import glob

f = glob.glob("q2\*.csv")
df = pd.read_csv(f[0],delimiter=',',names=['Name', 'Likescount'])
all_data = pd.DataFrame()
all_data = all_data.append(df,ignore_index=True)
print(type(all_data))
labels = all_data['Name']
print(labels)
sizes = all_data['Likescount']
index = np.arange(len(labels))
print(index)
#which defines the length of total plot
plt.figure(figsize=(20, 3))
#which defines width of bar
plt.bar(index, sizes, width=0.3)
plt.xlabel('Name')
plt.ylabel('Likescount')
#which defines font size of xticks
plt.xticks(index, labels,fontsize=7)
plt.title('cUsername and likes count who used apple in their description')

plt.show()

#bar_graph()
