import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

import cufflinks as cf
cf.go_offline()

from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler,PolynomialFeatures
from sklearn.linear_model import LinearRegression
#%matplotlib inline

from folium import Choropleth, Circle, Marker
from folium.plugins import HeatMap, MarkerCluster
import folium
from folium.plugins import FastMarkerCluster

import plotly.offline as py
import plotly.graph_objs as go

import plotly.express as px

df = pd.read_parquet('gs://stack-labs-list/curated/df_main')

def transformar_estado(valor):
    if valor == 'AC':
        return 'Norte'
    elif valor == 'AP':
        return 'Norte'
    elif valor == 'AM':
        return 'Norte'
    elif valor == 'PA':
        return 'Norte'
    elif valor == 'RO':
        return 'Norte'
    elif valor == 'RR':
        return 'Norte'
    elif valor == 'TO':
        return 'Norte'
    elif valor == 'AL':
        return 'Nordeste'
    elif valor == 'BA':
        return 'Nordeste'
    elif valor == 'CE':
        return 'Nordeste'
    elif valor == 'MA':
        return 'Nordeste'
    elif valor == 'PB':
        return 'Nordeste'
    elif valor == 'PE':
        return 'Nordeste'
    elif valor == 'PI':
        return 'Nordeste'
    elif valor == 'RN':
        return 'Nordeste'
    elif valor == 'SE':
        return 'Nordeste'  
    elif valor == 'DF':
        return 'Centro-Oeste'
    elif valor == 'GO':
        return 'Centro-Oeste'
    elif valor == 'MT':
        return 'Centro-Oeste'
    elif valor == 'MS':
        return 'Centro-Oeste'
    elif valor == 'ES':
        return 'Sudeste'
    elif valor == 'RJ':
        return 'Sudeste'
    elif valor == 'MG':
        return 'Sudeste'
    elif valor == 'SP':
        return 'Sudeste'
    else:
        return 'Sul'
      
df['customer_region'] = df['customer_state'].map(transformar_estado)
df['seller_region'] = df['seller_state'].map(transformar_estado)

df_1 = df[(df['customer_region'] == 'Nordeste') | (df['customer_region'] == 'Norte') | (df['customer_region'] == 'Centro-Oeste')]


df_customer_region = df_1[(df_1.seller_region != "Nordeste") & (df_1.seller_region != "Norte") & (df_1.seller_region != "Centro-Oeste")]


df_customer_region.to_parquet('gs://stack-labs-list/curated/df_customer_region')
df_1.to_parquet('gs://stack-labs-list/curated/df_1')

