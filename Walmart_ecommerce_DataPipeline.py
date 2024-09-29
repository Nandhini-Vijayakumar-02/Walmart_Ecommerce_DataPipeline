import pandas as pd 
import numpy as np 
import os
import logging

logging.basicConfig(format="%(process)d- %(levelname)s-%(message)s")
### Extract data from store df and parquet file and merge it
def extract(store_df, file):
    try:
     if not store_df.empty:
        parquet = pd.read_parquet(file)
        merged_df=  store_df.merge(parquet, how="inner", on="index")
        logging.info('Data extracted correctly')
     else:
        logging.warning('Data is empty')
    except Exception as e:
       logging.error('An error occured', e)
    return merged_df 

merged_df =extract(store_df, "'/parquet_file'")

### Transform data in the merged df

def transform(raw_data):
    try:
      if not raw_data.empty:
        raw_data.fillna({'CPI': raw_data['CPI'].mean(),
                          'Weekly_Sales': raw_data['Weekly_Sales'].mean(),
                          'Unemployment': raw_data['Unemployment'].mean()
                          }, inplace= True)
        raw_data["Date"] =pd.to_datetime(raw_data["Date"], format="%Y-%m-%d")
        raw_data["Month"] = raw_data["Date"].dt.month
        raw_data = raw_data.loc[raw_data["Weekly_Sales"]>10000, : ]
         # Drop unnecessary columns. Set axis = 1 to specify that the columns should be removed
        final_table_columns = ['Store_ID', 'Month', 'Dept', 'IsHoliday',
                                   'Weekly_Sales', 'CPI', 'Unemployment']
        raw_data = raw_data.drop(columns = [col for col in raw_data if col not in final_table_columns], axis=1)
        logging.info('Data transformed correctly')
      else:
        logging.warning('Data is empty')
    except Exception as e:
        logging.error('An error', e)
    return raw_data 

clean_data= transform(merged_df)


### To compute the average monthly sales 

def avg_monthly_sales(clean_data):
    try:
        if not clean_data.empty:
          holiday_sales= clean_data[["Month","Weekly_Sales"]]

          holiday_sales = (holiday_sales.groupby("Month")
          .agg(Avg_Sales=("Weekly_Sales", "mean"))
          .reset_index().round(2))
          logging.info('Data aggregated correctly')
        else:
           logging.warning('Df empty')
    except Exception as e:
           logging.error('An error', e)
    return holiday_sales

agg_data= avg_monthly_sales(clean_data)

### load the data and store it as csv files at some location
def load(clean_data, clean_data_file_path, agg_sales, agg_sales_file_path):
    try:
        if not ( clean_data.empty and agg_sales.empty):
          clean_data.to_csv(clean_data_file_path, index= False)
          agg_sales.to_scv(agg_sales_file_path, index= False)
          logging.info('Data loaded succesfully')
        else:
          logging.warning('Df is empty')
    except Exception as e:
           logging.error('Error', e)


load(clean_data, "/clean_data.csv", agg_data, "/agg_data.csv")


def validation(file_path):
   
    try:
      file_exists= os.path.exists(file_path)
      logging.info('{0} exists'.format(file_exists))
      if not file_exists:
         logging.error('Error', e)
         raise Exception(f"No file at the path {file_path}")
    except Exception as e :
        logging.error('Error occured while validating file', e)
       
         
validation("/clean_data.csv")
validation("/agg_data.csv") 


         
          
                      
   

         

