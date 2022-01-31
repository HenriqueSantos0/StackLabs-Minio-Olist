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

#Lendo arquivos csv         
df_customer = pd.read_csv('gs://stack-labs-list/landig/olist_customers_dataset.csv', sep=";")
df_geolocation = pd.read_csv('gs://stack-labs-list/landig/olist_geolocation_dataset.csv', sep=";")
df_order_items = pd.read_csv('gs://stack-labs-list/landig/olist_order_items_dataset.csv', sep=";")
df_order_payments = pd.read_csv('gs://stack-labs-list/landig/olist_order_payments_dataset.csv', sep=";")
df_order_reviews = pd.read_csv('gs://stack-labs-list/landig/olist_order_reviews_dataset.csv', sep=";")
df_orders = pd.read_csv('gs://stack-labs-list/landig/olist_orders_dataset.csv', sep=";")
df_products = pd.read_csv('gs://stack-labs-list/landig/olist_products_dataset.csv', sep=";")
df_sellers = pd.read_csv('gs://stack-labs-list/landig/olist_sellers_dataset.csv', sep=";")
df_category_name = pd.read_csv('gs://stack-labs-list/landig/product_category_name_translation.csv', sep=";")

#Salvando em arquivo parquet
df_customer.to_parquet('gs://stack-labs-list/processing/df_customer')
df_geolocation.to_parquet('gs://stack-labs-list/processing/df_geolocation')
df_order_items.to_parquet('gs://stack-labs-list/processing/df_order_items')
df_order_payments.to_parquet('gs://stack-labs-list/processing/df_order_payments')
df_order_reviews.to_parquet('gs://stack-labs-list/processing/df_order_reviews')
df_orders.to_parquet('gs://stack-labs-list/processing/df_orders')
df_products.to_parquet('gs://stack-labs-list/processing/df_products')
df_sellers.to_parquet('gs://stack-labs-list/processing/df_sellers')
df_category_name.to_parquet('gs://stack-labs-list/processing/df_category_name')

#Lendo arquivos em parquet
df_customer=pd.read_parquet('gs://stack-labs-list/processing/df_customer')
df_geolocation=pd.read_parquet('gs://stack-labs-list/processing/df_geolocation')
df_order_items=pd.read_parquet('gs://stack-labs-list/processing/df_order_items')
df_order_payments=pd.read_parquet('gs://stack-labs-list/processing/df_order_payments')
df_order_reviews=pd.read_parquet('gs://stack-labs-list/processing/df_order_reviews')
df_orders=pd.read_parquet('gs://stack-labs-list/processing/df_orders')
df_products=pd.read_parquet('gs://stack-labs-list/processing/df_products')
df_sellers=pd.read_parquet('gs://stack-labs-list/processing/df_sellers')
df_category_name=pd.read_parquet('gs://stack-labs-list/processing/df_category_name')

#Executando merge
df = df_orders.merge(df_order_items, on='order_id', how='left')
df = df.merge(df_order_payments, on='order_id', how='outer', validate='m:m')
df = df.merge(df_order_reviews, on='order_id', how='outer')
df = df.merge(df_products, on='product_id', how='outer')
df = df.merge(df_customer, on='customer_id', how='outer')
df = df.merge(df_sellers, on='seller_id', how='outer')

#Criando tabela fato
df=df[['customer_state', 'customer_city', 'customer_id', 'customer_unique_id', 'seller_state', 'seller_id', 'order_id', 'order_item_id', 'order_status', 'order_purchase_timestamp', 'order_approved_at', 'order_estimated_delivery_date', 'order_delivered_customer_date', 'freight_value', 'price']]

df.to_parquet('gs://stack-labs-list/curated/df_betha')


df['order_purchase_timestamp'] = pd.to_datetime(df['order_purchase_timestamp'])
df['order_purchase_month'] = df['order_purchase_timestamp'].dt.to_period('M').astype(str)

df['order_purchase_timestamp'] = pd.to_datetime(df['order_purchase_timestamp'])
df['order_approved_at'] = pd.to_datetime(df['order_approved_at'])
df['order_estimated_delivery_date'] = pd.to_datetime(df['order_estimated_delivery_date'])
df['order_delivered_customer_date'] = pd.to_datetime(df['order_delivered_customer_date'])

df['delivery_time'] = (df['order_delivered_customer_date'] - df['order_approved_at']).dt.total_seconds() / 86400
df['estimated_delivery_time'] = (df['order_estimated_delivery_date'] - df['order_approved_at']).dt.total_seconds() / 86400

df['order_freight_ratio'] = df.freight_value / df.price


df['diff_delivery_estimated'] = df['delivery_time'] - df['estimated_delivery_time']

#Precisa resolver~~~~~~~~
df['data_delivery_products'] = df['diff_delivery_estimated']> 0

#~~~~~~~~


df.to_parquet('gs://stack-labs-list/curated/df_main')














      
      
