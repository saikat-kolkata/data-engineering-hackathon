#import necessary libraries

import pandas as pd 
import numpy as np
from datetime import datetime
import warnings
warnings.filterwarnings("ignore")

class Pipeline:
    def __init__(self, lastdayfrom):

        self.lastdayfrom = lastdayfrom
    
    def clean_dataset(self,visitor_log_data):
        #This function clean the dataset, remove anomalies and NULL values
        print('Cleaning of data started')

        #Field City and Country & webClientID in visitor_log_data is not necessary so drop them
        visitor_log_data.drop(['City','Country','webClientID'],axis=1,inplace=True)

        #drop the rows where UserID is NULL
        visitor_log_data.dropna(subset=['UserID'],inplace=True)

        #sorted the dataframe by UserID and then ffill VisitDateTime column to fill the NULL

        visitor_log_data.sort_values(by=['UserID','ProductID'],inplace = True)
        visitor_log_data['VisitDateTime'].ffill(inplace=True)
        visitor_log_data.sort_index(inplace=True)

        #transfer unix datetime to normal datetime
        visitor_log_data['VisitDateTime'] = visitor_log_data['VisitDateTime'].apply(lambda x: pd.to_datetime(int(x)) 
                                                                              if ('-' not in str(x)) else x)
        
        #first we sort the data based on 'UserID','VisitDateTime' and then we ffill the NULLs of Activity

        visitor_log_data.sort_values(by=['UserID','VisitDateTime'],inplace = True)
        visitor_log_data['Activity'].ffill(inplace=True)
        visitor_log_data['ProductID'].bfill(inplace=True)
        visitor_log_data.sort_index(inplace=True)

        #change string values in columns to upper case for uniformity

        visitor_log_data['Activity'] = visitor_log_data['Activity'].str.upper()
        visitor_log_data['Browser'] = visitor_log_data['Browser'].str.upper()
        visitor_log_data['OS'] = visitor_log_data['OS'].str.upper()
        visitor_log_data['ProductID'] = visitor_log_data['ProductID'].str.upper()

        print('Data cleaning complete')

        return visitor_log_data

    def slice_dataframe_on_date(self,visitor_log_data,no_of_days):
        #This function filter dataframe as per requirement of days
        return visitor_log_data[visitor_log_data['VisitDateTime'] >= (pd.to_datetime(self.lastdayfrom) - pd.Timedelta(days=no_of_days))]

    def no_of_days_visited_7_days(self,visitor_log_data):
        #this function creates dictionary to map with userid 
        #and creates no_of_days_visited_7_days feature

        last_seven_days = self.slice_dataframe_on_date(visitor_log_data,7)
        last_seven_days['days'] = last_seven_days['VisitDateTime'].apply(lambda x: x.day)
        
        # create dictionary with userid and How many days a user was active on platform in the last 7 days

        temp_7_active_usr=last_seven_days[['UserID','days']].groupby(['UserID','days']).count().reset_index()
        df_7_active_usr = temp_7_active_usr.groupby('UserID').count().reset_index()
        dic_map_usr_7_Days = pd.Series(df_7_active_usr['days'].values,index=df_7_active_usr['UserID']).to_dict()

        print ('No_of_days_Visited_7_Days feature created')
        return dic_map_usr_7_Days


    def no_of_products_viewed_15_days(self,visitor_log_data):
        #this function creates dictionary to map with userid 
        #and creates no_of_products_viewed_15_days feature
        last_15_days = self.slice_dataframe_on_date(visitor_log_data,15)
        
        # create dictionary with userid and How many product a user viewed on platform in the last 15 days

        temp_15days_prodct = last_15_days[['UserID','ProductID']].groupby(['UserID','ProductID']).count().reset_index()
        df_15days_product = temp_15days_prodct.groupby('UserID')['ProductID'].count().reset_index()
        dic_map_15_Days_product = pd.Series(df_15days_product['ProductID'].values,index=df_15days_product['UserID']).to_dict()

        print ('No_Of_Products_Viewed_15_Days feature created')
        return dic_map_15_Days_product

    def user_vintage(self,user_data):
        #this function calculate Vintage (In Days) of the user as of today
        user_data['User_Vintage']= (pd.to_datetime(self.lastdayfrom) - pd.to_datetime(user_data['Signup Date']).dt.tz_localize(None))
        user_data['User_Vintage'] = user_data['User_Vintage'].dt.days
        user_vinta_dic = pd.Series(user_data['User_Vintage'].values,index=user_data['UserID']).to_dict()

        print ('User_Vintage feature created')
        return user_vinta_dic

    def most_viewed_product_15_days(self,visitor_log_data):
        #this function calculate Most frequently viewed (page loads) product by the user in the last 15 days
        last_15_days = self.slice_dataframe_on_date(visitor_log_data,15)  #filter data for 15days
        view15_product = last_15_days[last_15_days['Activity'] == 'PAGELOAD'] #filter datawith PAGELOAD
        # in 15 days which products are viewed how many time by user
        temp_most_viewd = view15_product[['UserID','ProductID','Activity']].groupby(['UserID','ProductID'])['Activity'].count().reset_index()
        #most viewed product
        most_viewd_15=temp_most_viewd.iloc[temp_most_viewd.groupby('UserID').apply(lambda x: x['Activity'].idxmax())]
        #dictionary for transformation
        most_viewed_prdct_dic = pd.Series(most_viewd_15['ProductID'].values,index=most_viewd_15['UserID']).to_dict()

        print ('Most_Viewed_product_15_Days feature created')
        return most_viewed_prdct_dic

    def most_active_os(self,visitor_log_data):
        #this function calculate Most Frequently used OS by user
        active_os_temp = visitor_log_data[['UserID','OS','Browser']].groupby(['UserID','OS'])['Browser'].count().reset_index()
        #most active os
        active_os=active_os_temp.iloc[active_os_temp.groupby('UserID').apply(lambda x: x['Browser'].idxmax())]
        #create dic to map
        active_os_dic = pd.Series(active_os['OS'].values,index=active_os['UserID']).to_dict()

        print ('Most_Active_OS feature created')
        return active_os_dic

    def recently_viewed_product(self,visitor_log_data):
        #This function Most recently viewed (page loads) product by the user. If a user has not viewed any product then put it as Product101.
        #recently viewed products
        recently_viewed_products = visitor_log_data[['UserID','ProductID','VisitDateTime']][visitor_log_data['Activity']=='PAGELOAD'].groupby(['UserID','ProductID'])['VisitDateTime'].agg('max').reset_index()
        #most recent view product
        temp_recent_prodct = recently_viewed_products.iloc[recently_viewed_products.groupby('UserID').apply(lambda x: x['VisitDateTime'].idxmax())]
        #create dic to map
        temp_recent_prodct_dic = pd.Series(temp_recent_prodct['ProductID'].values,index=temp_recent_prodct['UserID']).to_dict()

        print ('Recently_Viewed_Product feature created')
        return temp_recent_prodct_dic

    def pageloads_last_7_days(self,visitor_log_data):
        #This function Count of Page loads in the last 7 days by the user

        last_seven_days = self.slice_dataframe_on_date(visitor_log_data,7)
        Pageloads_last_7 = last_seven_days[last_seven_days['Activity']=='PAGELOAD'].groupby('UserID')['Activity'].count().reset_index()
        Pageloads_last_7_dic = pd.Series(Pageloads_last_7['Activity'].values,index=Pageloads_last_7['UserID']).to_dict()
        
        print ('Pageloads_last_7_days feature created')
        return Pageloads_last_7_dic

    def clicks_last_7_days(self,visitor_log_data):
        #This function Count of Clicks in the last 7 days by the user
        last_seven_days = self.slice_dataframe_on_date(visitor_log_data,7)
        CLICK_last_7 = last_seven_days[last_seven_days['Activity']=='CLICK'].groupby('UserID')['Activity'].count().reset_index()
        CLICK_last_7_dic = pd.Series(CLICK_last_7['Activity'].values,index=CLICK_last_7['UserID']).to_dict()

        print ('Clicks_last_7_days feature created')
        return CLICK_last_7_dic



      
    # ====   master function that do feature engineering ==== #
    
    def fit(self,visitor_log_data,user_data,input_feature):
        #clean the dataset, call fn -> clean_dataset
        visitor_log_data = self.clean_dataset(visitor_log_data)

        #create No_of_days_Visited_7_Days, call fn-> no_of_days_visited_7_days
        #filled input_feature mapped with user active in 7 days 
        input_feature['No_of_days_Visited_7_Days']= input_feature['UserID'].map(self.no_of_days_visited_7_days(visitor_log_data))
        #NULL value filled with '0'
        input_feature['No_of_days_Visited_7_Days'].fillna(0,inplace=True) 

        #feature No_Of_Products_Viewed_15_Days added
        input_feature['No_Of_Products_Viewed_15_Days']= input_feature['UserID'].map(self.no_of_products_viewed_15_days(visitor_log_data))
        #those user who were not viewed products put zero for them
        input_feature['No_Of_Products_Viewed_15_Days'].fillna(0,inplace=True) 

        #feature User_Vintage added
        input_feature['User_Vintage']= input_feature['UserID'].map(self.user_vintage(user_data))

        #feature Most_Viewed_product_15_Days added
        input_feature['Most_Viewed_product_15_Days']= input_feature['UserID'].map(self.most_viewed_product_15_days(visitor_log_data))
        #nulls will fill up with Product101
        input_feature['Most_Viewed_product_15_Days'].fillna('Product101',inplace=True)

        #feature Most_Active_OS added
        input_feature['Most_Active_OS']= input_feature['UserID'].map(self.most_active_os(visitor_log_data))
        
        # feature Recently_Viewed_Product added
        input_feature['Recently_Viewed_Product']= input_feature['UserID'].map(self.recently_viewed_product(visitor_log_data))
        #fillup the nan
        input_feature['Recently_Viewed_Product'].fillna('Product101',inplace=True)

        # feature Pageloads_last_7_days added
        input_feature['Pageloads_last_7_days']= input_feature['UserID'].map(self.pageloads_last_7_days(visitor_log_data))
        #fill null with 0
        input_feature['Pageloads_last_7_days'].fillna(0,inplace=True)

        # feature Clicks_last_7_days added
        input_feature['Clicks_last_7_days']= input_feature['UserID'].map(self.clicks_last_7_days(visitor_log_data))
        #fill null with 0
        input_feature['Clicks_last_7_days'].fillna(0,inplace=True)

        input_feature.to_csv('Result.csv',index=None)
        print('===== File Generated Successfully =====')
        # print(visitor_log_data.head())
        # print(user_data.head())
        # print(input_feature.head())

        
