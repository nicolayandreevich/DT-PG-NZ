import pandas as pd
import py7zr
import calendar
import os

# %% load dict`s
features_group = pd.read_excel(r'.\codes\feature_groups.xlsx')
demo_order = pd.read_excel(r'.\codes\demo_order.xlsx')
periods = pd.read_excel(r'.\codes\periods.xlsx', sheet_name ='period_lbl')
time_period_type = pd.read_excel(r'.\codes\periods.xlsx', sheet_name ='time_period_type')
year = pd.read_excel(r'.\codes\periods.xlsx', sheet_name ='year')
segments_order = pd.read_excel(r'.\codes\segments_order.xlsx')
example = pd.read_excel(r'.\codes\Example.xlsx', sheet_name = 'example')
legends = pd.read_excel(r'.\codes\Example.xlsx', sheet_name = 'legends')

cat_dict = {'br':'MALE B&R', 'deterg': 'Laundry Detergents','diapers': 'Diapers','femcare': 'Feminine Care', 
                'haircond': 'Hair Conditioners', 'shampoo':'Shampoos' }
time_period_type_dict ={ 24 :'2MATs/ 104 we', 12:'MAT/ 52 we', 3:'3MMT/ 12we' }

columns_dict = {}
for m, c in legends.loc[legends['mask'].notna(), ['mask','Colum' ]].values:
    columns_dict[m] = c  




# %% make_functions

def get_period_lbl(time_period_type, year, month):
    return f"{time_period_type.split('/')[0]}: {calendar.month_abbr[month]} {year}"  



def get_df_in_v2(path, file):
    categoty  = cat_dict[file.split('_')[0]]
    type_of_file = file.split('_')[1]
    df_in = pd.read_parquet(f'{path}\\{file}')
    df_in = df_in.rename({'Category Name':'Product Name', 'Buyer Group Name':'buyers_gr_label'
                           }, axis=1 )
    df_in['Category Name'] = [categoty] * len( df_in)
    
    col_to_int = ['duration', 'year', 'month']
    for col in col_to_int:
        df_in[col] = df_in[col].astype(int)
    
    df_in['time_period_type'] = df_in['duration'].map(time_period_type_dict) 
    #Ниже нужно оптимизировать функцию, по 10 секунд выполняется
    df_in['period_lbl'] = df_in[['time_period_type', 'year', 'month']].apply(lambda row:get_period_lbl(*row), axis=1)
    print(sum(df_in['time_period_type'].isnull()))   
    print(df_in.shape)
    df_in = df_in.merge(features_group, on =['Category Name', 'Product Name'], how = 'left') 
    print(df_in.shape)
    df_in = df_in.merge(demo_order, on =['buyers_gr_label'], how = 'left')
    print(df_in.shape)
    df_in = df_in.merge(periods, on =['period_lbl'], how = 'left') 
    print(df_in.shape)
    df_in = df_in.merge(time_period_type, on =['time_period_type'], how = 'left')
    print(df_in.shape)
    df_in = df_in.merge(year, on =['year'], how = 'left')
    print(df_in.shape)
    df_in = df_in.merge(segments_order, on =['Segment'], how = 'left')
    print(df_in.shape)
    df_in.columns =[col.lower().replace(' ','_')  for col in  df_in.columns]
    df_in = df_in.rename(columns_dict, axis=1)
    return df_in


#%% load data file with name 'out-for-datatile.7z' 


archive = py7zr.SevenZipFile('../data_in/out-for-datatile.7z', 'r')  
archive.extractall(path="/tmp")
path = '/tmp/out-for-datatile'
files = os.listdir(path)
files


df_in_one = get_df_in_v2(path, files[0])

print(df_in_one.columns )