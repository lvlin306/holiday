#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec 23 17:21:37 2020

@author: lvlinlin
"""

import csv
import pandas as pd
file = "/Users/lvlinlin/Desktop/tedtalk/ted_main.csv"
 
with open(file,'r', encoding = 'UTF-8') as f :
    # TODO
    # 使用csv.DictReader读取文件中的信息
    reader = csv.DictReader(f)
    name = []
    views = []
    speaker_occupation = []
    for row in reader :
        # TODO 
        # 将 'main_speaker' 、'views'中的每个元素以整型数据分别添加在相应的列表中
        name.append(row['main_speaker'])
        views.append(row['views'])
        speaker_occupation.append(row['speaker_occupation'])
        
#字典中的key值即为csv中列名
data = {'name':name,'speaker_occupation':speaker_occupation,'views':views}
dataframe = pd.DataFrame(data)
dataframe.to_csv(r'data1.csv')
 
with open(file,'r', encoding = 'UTF-8') as f :
    # TODO
    # 使用csv.DictReader读取文件中的信息
    reader = csv.DictReader(f)
    name1 = []
    name = []
    x = []
    for row in reader :
        name.append(row['name'])
    for i in range(0,len(name)):
        if i + 1 < len(name):
            if name[i] == name[i + 1]:
                x.append(name[i])
            else:
                x.append(name[i])
                name1.append(x)
                x = []
        else:
            x.append(name[len(name) - 1])
            name1.append(x)
    print(name1)

ted = pd.read_csv("/Users/lvlinlin/Desktop/tedtalk/ted_main.csv")
occupation_df = ted.groupby('speaker_occupation').count().reset_index()[['speaker_occupation', 'views']]
data = ted[ted['speaker_occupation'].isin(occupation_df.head(10)['speaker_occupation'])]
print(data)


occupation_df = ted.groupby('speaker_occupation').count().reset_index()[['speaker_occupation', 'comments']]
occupation_df.columns = ['演讲者职业', '演讲次数']
occupation_df = occupation_df.sort_values('演讲次数', ascending = False)
occupation_df.head(20)
print(occupation_df.head(20))
