
# coding: utf-8

# # car_inspections & auctions
# 
# Aim: Predict car price & returned rate
# 
# Original files (2): (FCG...Auctions.csv) & (FCG...Inspections.csv)<br>
# Auctions.csv is structured, but Inspections.csv is quite dirty
# 
# <b>Preprocessing</b><br>
# (FCG..Inspections.csv) : json --> pandas<br>
# 2 columns about the score of the used car, in json object<br>
# --> This is saved as pandas, then save it as inspection_report.csv, category_scores.csv
# 
# <b>Merging files</b><br>
# inspection_full.csv (mainly structured FCG...inspections.csv)<br>
# Merge all csv files with join (car_id is overlapping for all csv file)
# 

# # load data & libraries

# In[1]:


import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import json


# In[2]:


# file name
auction_fname = 'FCG Germany GmbH_Data Scientist_case study_Auctions.csv'
inspection_fname = 'FCG Germany GmbH_Data Scientist_case study_Inspections.csv'


# In[3]:


auction_data = pd.read_csv(auction_fname)
inspection_data = pd.read_csv(inspection_fname)


# ## Feature engineering

# # Inspection extract
# 
# inspection_data['category_scores'] (json) ==> category_scores (pandas)

# In[4]:


# Preassign inspect_allcar
category_scores_pd = pd.DataFrame()


# In[5]:


# Extract the first car_id's inspection score
# --> assign as standard name
scores_car1 = json.loads(inspection_data['category_scores'][0])
scores_pd = pd.DataFrame.from_dict(scores_car1)
stand_name = scores_pd.id


# In[6]:


# Extract for all car
# each car
for car in range(len(inspection_data)):
    scores_dict = json.loads(inspection_data['category_scores'][car])
    scores_onecar = pd.DataFrame.from_dict(scores_dict)
    
    # each features of the car
    for f in range(len(stand_name)):
        # run only when inspection feature exists, some car has no inspection
        if len(scores_onecar)>1: 
            if scores_onecar['id'][f] == stand_name[f]: # when feature name match with standard name
                category_scores_pd.at[car, stand_name[f]] = scores_onecar.score[f]
        else: # when inspection is missing assign NaN
            category_scores_pd.at[car, stand_name[f]] = 'NaN'
            


# Add car_id column
# (This is to later merge with other files: car_id has to match with other files)

# In[7]:


category_scores = category_scores_pd.copy()


# In[8]:


category_scores['car_id'] = inspection_data['car_id']


# In[9]:


cols = category_scores.columns.tolist()
cols = cols[-1:] + cols[:-1]
category_scores = category_scores[cols]


# Save inspect_cars

# In[10]:


category_scores.to_csv('category_scores.csv', encoding = 'utf-8', index=False)


# ## Report extract

# In[11]:


# Preassign inspect_allcar
report_allcar_pd = pd.DataFrame()


# In[12]:


# Extract the first car_id's inspection score
# --> assign as standard name
report_car1 = json.loads(inspection_data['inspection_report'][0])
pd_car1 = pd.DataFrame(list(report_car1.items()))
stand_report = pd_car1.loc[:,0]


# In[14]:


# Extract for all car
# each car
for car in range(len(inspection_data)):
    report_onecar=json.loads(inspection_data['inspection_report'][car])
    report_onecar_pd=pd.DataFrame.from_dict(report_onecar, orient = 'index')
    
    # each features of the car
    for f in range(len(stand_report)):
        if len(report_onecar_pd) >1:# run when there is a inspection feature
            if len(report_onecar_pd.loc[:,0].keys()) > f:
                if report_onecar_pd.loc[:,0].keys()[f] == stand_report[f]: # when feature name match
                    report_allcar_pd.at[car, stand_report[f]] = report_onecar_pd.values[f][0]
                else:
                    report_allcar_pd.at[car, stand_report[f]] = 'NaN'  
            else:
                report_allcar_pd.at[car, stand_report[f]] = 'NaN'
        else: # if feature part is empty
            report_allcar_pd.at[car, stand_report[f]] = 'NaN'


# In[15]:


report_allcar_pd.shape, inspection_data.shape


# In[16]:


report_cars = report_allcar_pd.copy()


# In[17]:


# add car_id
report_cars['car_id'] = inspection_data['car_id']


# In[18]:


cols = report_cars.columns.tolist()
cols = cols[-1:] + cols[:-1]
report_cars = report_cars[cols]


# In[21]:


## save report_cars
report_cars.to_csv('inspection_report.csv', encoding = 'utf-8', index=False)


# ## Merge files unorganized inspection into clean inspect_full (pandas)

# In[22]:


inspect_jsons = pd.concat([category_scores, report_allcar_pd],axis=1)


# In[31]:


inspect_wo_json = inspection_data.drop(['category_scores', 'inspection_report'], axis=1)


# In[33]:


inspect_full = pd.concat([inspect_wo_json,category_scores_pd,report_allcar_pd],axis=1)


# In[35]:


## save report_cars
inspect_full.to_csv('inspection_full.csv', encoding = 'utf-8', index=False)


# ## Merge inspect + auction (join!!)

# In[43]:


inspect_auction_all = pd.merge(auction_data, inspect_full, on='car_id', how='outer')


# In[47]:


## save all
inspect_auction_all.to_csv('car_full.csv', encoding = 'utf-8', index=False)

