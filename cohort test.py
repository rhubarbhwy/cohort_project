#!/usr/bin/env python
# coding: utf-8

import numpy as np
import pandas as pd
import json

#import first json file
d = pd.read_json('bublywater-2023-03-01-2023-09-01-94.json')
data = d['data']

n=0 
dic={"id":[],"url":[],"commentsCount":[],"likesCount":[],"timestamp":[],"shortCode":[], "caption":[]}
for n in range(len(data)):
        dic["id"].append(data[n]['id'])
        dic["url"].append(data[n]['url'])
        dic["commentsCount"].append(data[n]['commentsCount'])
        dic["likesCount"].append(data[n]['likesCount'])
        dic["timestamp"].append(data[n]['timestamp'])
        dic["caption"].append(data[n]['caption'])
        n+=1

df = pd.DataFrame.from_dict(dic, orient='index')
df = df.transpose()
df['source']='instagram'
df['brand']='bublywater'

#import another json file
d2=pd.read_json('drinkspindrift-2023-03-01-2023-09-01-101.json')
data2 = d2['data']
data2

n=0 
dic2={"id":[],"url":[],"commentsCount":[],"likesCount":[],"timestamp":[],"shortCode":[], "caption":[]}
for n in range(len(data2)):
        dic2["id"].append(data[n]['id'])
        dic2["url"].append(data[n]['url'])
        dic2["commentsCount"].append(data[n]['commentsCount'])
        dic2["likesCount"].append(data[n]['likesCount'])
        dic2["timestamp"].append(data[n]['timestamp'])
        dic2["caption"].append(data[n]['caption'])
        n+=1
df2 = pd.DataFrame.from_dict(dic2, orient='index')
df2 = df2.transpose()
df2['source']='instagram'
df2['brand']='drinkspindrift'

result = pd.concat([df, df2])




