import matplotlib
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from dask.optimization import inline
%matplotlib inline
import dask.dataframe as ddf
from pandas import Series, DataFrame
import seaborn as sn
import plotly.express as px

df1 = pd.read_csv(r'Death_2001-2005.txt', sep='\t',dtype={"County Code": str})
df1 = df1.drop("Notes",1)
df1['Deaths']=df1['Deaths'].astype(int)
df1['Population']=df1['Population'].astype(int)
df1['SuicideDeathRate'] = (df1['Deaths'] / df1['Population'])*100000
df1.head()

x1 = df1['SuicideDeathRate']
bins = np.arange(0, 100, 2)
plt.figure(figsize=(16,9))
ax = sn.histplot(x1, bins = bins)
plt.xticks(bins) # set bins value
plt.title("Suicide Death Rate from 2001 to 2005")
plt.show()

bins = np.arange(0, 100, 2)
plt.figure(figsize=(16,9))
ax = sn.distplot(x1, bins = bins, hist = False)
plt.xticks(bins) # set bins value
plt.title("Normal Distribution of Suicide Death Rate from 2001 to 2005")
plt.show()

from urllib.request import urlopen
import json
with urlopen('https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json') as response:
    counties = json.load(response)

import pandas as pd
#df = pd.read_csv("https://raw.githubusercontent.com/plotly/datasets/master/fips-unemp-16.csv",
#                   dtype={"fips": str})

import plotly.express as px

fig = px.choropleth_mapbox(df1, geojson=counties, locations='County Code', color='SuicideDeathRate',
                           color_continuous_scale="Viridis",
                           range_color=(0, 30),
                           mapbox_style="carto-positron",
                           zoom=3, center = {"lat": 37.0902, "lon": -95.7129},
                           opacity=0.5,
                           labels={'unemp':'unemployment rate'}
                          )
fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
fig.show()

df2 = pd.read_csv(r'Death_2006-2010.txt', sep='\t',dtype={"County Code": str})
df2 = df2.drop("Notes",1)
df2['Deaths']=df2['Deaths'].astype(int)
df2['Population']=df2['Population'].astype(int)
df2['SuicideDeathRate'] = (df2['Deaths'] / df2['Population'])*100000
df2.head()


x2 = df2['SuicideDeathRate']
bins = np.arange(0, 100, 2)
plt.figure(figsize=(16,9))
ax = sn.histplot(x2, bins = bins)
plt.xticks(bins) # set bins value
plt.title("Suicide Death Rate from 2006 to 2010")
plt.show()

bins = np.arange(0, 100, 2)
plt.figure(figsize=(16,9))
ax = sn.distplot(x2, bins = bins, hist = False)
plt.xticks(bins) # set bins value
plt.title("Normal Distribution of Suicide Death Rate from 2006 to 2010")
plt.show()


from urllib.request import urlopen
import json
with urlopen('https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json') as response:
    counties = json.load(response)

import pandas as pd
#df = pd.read_csv("https://raw.githubusercontent.com/plotly/datasets/master/fips-unemp-16.csv",
#                   dtype={"fips": str})

import plotly.express as px

fig = px.choropleth_mapbox(df2, geojson=counties, locations='County Code', color='SuicideDeathRate',
                           color_continuous_scale="Viridis",
                           range_color=(0, 30),
                           mapbox_style="carto-positron",
                           zoom=3, center = {"lat": 37.0902, "lon": -95.7129},
                           opacity=0.5,
                           labels={'unemp':'unemployment rate'}
                          )
fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
fig.show()

df3 = pd.read_csv(r'Death_2011-2015.txt', sep='\t',dtype={"County Code": str})
df3 = df3.drop("Notes",1)
df3['Deaths']=df3['Deaths'].astype(int)
df3['Population']=df3['Population'].astype(int)
df3['SuicideDeathRate'] = (df3['Deaths'] / df3['Population'])*100000
df3.head()


x3 = df3['SuicideDeathRate']
bins = np.arange(0, 100, 2)
plt.figure(figsize=(16,9))
ax = sn.histplot(x3, bins = bins)
plt.xticks(bins) # set bins value
plt.title("Suicide Death Rate from 2006 to 2010")
plt.show()

bins = np.arange(0, 100, 2)
plt.figure(figsize=(16,9))
ax = sn.distplot(x3, bins = bins, hist = False)
plt.xticks(bins) # set bins value
plt.title("Normal Distribution of Suicide Death Rate from 2011 to 2015")
plt.show()


from urllib.request import urlopen
import json
with urlopen('https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json') as response:
    counties = json.load(response)

import pandas as pd
#df = pd.read_csv("https://raw.githubusercontent.com/plotly/datasets/master/fips-unemp-16.csv",
#                   dtype={"fips": str})

import plotly.express as px

fig = px.choropleth_mapbox(df3, geojson=counties, locations='County Code', color='SuicideDeathRate',
                           color_continuous_scale="Viridis",
                           range_color=(0, 30),
                           mapbox_style="carto-positron",
                           zoom=3, center = {"lat": 37.0902, "lon": -95.7129},
                           opacity=0.5,
                           labels={'unemp':'unemployment rate'}
                          )
fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
fig.show()

df4 = pd.read_csv(r'Death_2016-2019.txt', sep='\t',dtype={"County Code": str})
df4 = df4.drop("Notes",1)
df4['Deaths']=df4['Deaths'].astype(int)
df4['Population']=df4['Population'].astype(int)
df4['SuicideDeathRate'] = (df4['Deaths'] / df4['Population'])*100000
df4.head()


x4 = df4['SuicideDeathRate']
bins = np.arange(0, 100, 2)
plt.figure(figsize=(16,9))
ax = sn.histplot(x4, bins = bins)
plt.xticks(bins) # set bins value
plt.title("Suicide Death Rate from 2006 to 2010")
plt.show()

bins = np.arange(0, 100, 2)
plt.figure(figsize=(16,9))
ax = sn.distplot(x4, bins = bins, hist = False)
plt.xticks(bins) # set bins value
plt.title("Normal Distribution of Suicide Death Rate from 2006 to 2010")
plt.show()


from urllib.request import urlopen
import json
with urlopen('https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json') as response:
    counties = json.load(response)

import pandas as pd
#df = pd.read_csv("https://raw.githubusercontent.com/plotly/datasets/master/fips-unemp-16.csv",
#                   dtype={"fips": str})

import plotly.express as px

fig = px.choropleth_mapbox(df4, geojson=counties, locations='County Code', color='SuicideDeathRate',
                           color_continuous_scale="Viridis",
                           range_color=(0, 30),
                           mapbox_style="carto-positron",
                           zoom=3, center = {"lat": 37.0902, "lon": -95.7129},
                           opacity=0.5,
                           labels={'unemp':'unemployment rate'}
                          )
fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
fig.show()

ur_files = ddf.read_csv("PM2.5_2019.csv", dtype={'CBSA_CODE': 'float64'})
df_pm = ur_files.compute()
df_pm.head()

df_total = df3.drop(['Deaths', 'Population', 'Crude Rate'], axis=1)
df_total

df_total.rename( columns = {'SuicideDeathRate': 'SuicideRate_2011-2015'}, inplace = True)
df_total.head()

df_total = df_total.merge(df1, how = 'left', on = 'County')
df_total = df_total.drop(['County Code_y', 'Deaths', 'Population', 'Crude Rate'], axis=1)
df_total.rename( columns = {'SuicideDeathRate': 'SuicideRate_2001-2005'}, inplace = True)
df_total = df_total.merge(df2, how = 'left', on ='County')
df_total = df_total.drop(['County Code', 'Deaths', 'Population', 'Crude Rate'], axis=1)
df_total.rename( columns = {'SuicideDeathRate': 'SuicideRate_2006-2010'}, inplace = True)
df_total = df_total.merge(df4, how = 'left', on = 'County')
df_total = df_total.drop(['County Code', 'Deaths', 'Population', 'Crude Rate'], axis=1)
df_total.rename( columns = {'SuicideDeathRate': 'SuicideRate_2016-2019'}, inplace = True)
df_total.rename( columns = {'County Code_x': 'County Code'}, inplace = True)
df_total = df_total[['County', 'County Code', 'SuicideRate_2001-2005', 'SuicideRate_2006-2010', 'SuicideRate_2011-2015', 'SuicideRate_2016-2019']]


df_total['Difference 1'] = df_total['SuicideRate_2006-2010'] - df_total['SuicideRate_2001-2005']
df_total['Difference 2'] = df_total['SuicideRate_2011-2015'] - df_total['SuicideRate_2006-2010']
df_total['Difference 3'] = df_total['SuicideRate_2016-2019'] - df_total['SuicideRate_2011-2015']

x = sum(df_total['Difference 1'] > 0)
y = sum(df_total['Difference 2'] < 0)
labels = 'Increase', 'Decrease'
sizes = [x, y]
explode = (0, 0.1)
fig1, ax1 = plt.subplots()
ax1.pie(sizes, explode=explode, labels=labels, autopct='%1.1f%%',
        shadow=True, startangle=90)
ax1.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
plt.show()


mean1 = df_total['SuicideRate_2001-2005'].agg('mean')
def getValue(value):
    if (value - mean1 > 0):
        return 1
    else:
        return -1

from urllib.request import urlopen
import json
with urlopen('https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json') as response:
    counties = json.load(response)

import pandas as pd
#df = pd.read_csv("https://raw.githubusercontent.com/plotly/datasets/master/fips-unemp-16.csv",
#                   dtype={"fips": str})

import plotly.express as px

fig = px.choropleth_mapbox(df_total, geojson=counties, locations='County Code', color='CompareToAverage1',
                           color_discrete_sequence=["red", "blue"],
                           mapbox_style="carto-positron",
                           zoom=3, center = {"lat": 37.0902, "lon": -95.7129},
                           opacity=0.5
                          )
fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
fig.show()

from urllib.request import urlopen
import json
with urlopen('https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json') as response:
    counties = json.load(response)

import pandas as pd
#df = pd.read_csv("https://raw.githubusercontent.com/plotly/datasets/master/fips-unemp-16.csv",
#                   dtype={"fips": str})

import plotly.express as px

fig = px.choropleth_mapbox(df_total, geojson=counties, locations='County Code', color='CompareToAverage1', hover_name = 'CompareToAverage1',
                           color_discrete_map= {
                               'Higher': 'Red',
                               'Lower': 'Blue'
                           },
                           mapbox_style="carto-positron",
                           zoom=3, center = {"lat": 37.0902, "lon": -95.7129},
                           opacity=0.5
                          )
fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
fig.show()

num_higher1 = df_total['CompareToAverage1'].value_counts()['Higher']
num_lower1 = df_total['CompareToAverage1'].value_counts()['Lower']
num_higher2 = df_total['CompareToAverage2'].value_counts()['Higher']
num_lower2 = df_total['CompareToAverage2'].value_counts()['Lower']
num_higher3 = df_total['CompareToAverage3'].value_counts()['Higher']
num_lower3 = df_total['CompareToAverage3'].value_counts()['Lower']
num_higher4 = df_total['CompareToAverage4'].value_counts()['Higher']
num_lower4 = df_total['CompareToAverage4'].value_counts()['Lower']


data = {'Class': ['Higher', 'Lower', 'Higher', 'Lower','Higher', 'Lower','Higher', 'Lower'],
        'Period': ['Period 1', 'Period 1', 'Period 2', 'Period 2', 'Period 3', 'Period 3', 'Period 4', 'Period 4'],
        'NumberOfCounties': [num_higher1, num_lower1, num_higher2, num_lower2, num_higher3, num_lower3, num_higher4, num_lower4]}
df_num = pd.DataFrame(data)
df_num

sn.catplot(x="Period", y="NumberOfCounties", hue="Class", kind="bar", data=df_num)


df_increase = df_total.iloc[:, [0, 1, 2, 5]]
df_increase = df_increase.dropna(how = 'any')
df_increase

df_increase['IncreaseRate(%)'] = ((df_increase['SuicideRate_2016-2019'] - df_increase['SuicideRate_2001-2005'])/df_increase['SuicideRate_2001-2005'])*100
df_increase

NationalAverageIncreaseRate = df_increase.agg('mean')['IncreaseRate(%)']
df_range = df_increase.groupby(pd.cut(df_increase["IncreaseRate(%)"],np.arange(-60, 320, 10))).agg({"IncreaseRate(%)": "count"})
df_increase['IncreaseRange']= pd.qcut(df_increase["IncreaseRate(%)"],37, duplicates="drop")

from urllib.request import urlopen
import json
with urlopen('https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json') as response:
    counties = json.load(response)

import pandas as pd
#df = pd.read_csv("https://raw.githubusercontent.com/plotly/datasets/master/fips-unemp-16.csv",
#                   dtype={"fips": str})

import plotly.express as px

fig = px.choropleth_mapbox(df_increase, geojson=counties, locations='County Code', color='IncreaseRange',
                           color_continuous_scale="Viridis",
                           range_color=(0, 40),
                           mapbox_style="carto-positron",
                           zoom=3, center = {"lat": 37.0902, "lon": -95.7129},
                           opacity=0.5,
                           #labels={'unemp':'unemployment rate'}
                          )
fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
fig.show()

