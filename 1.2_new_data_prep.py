import pandas as pd
import calendar
from pathlib import Path
# import py7zr
# import os

# %% load dicts
features_group = pd.read_excel(r'.\codes\feature_groups.xlsx')
demo_order = pd.read_excel(r'.\codes\demo_order.xlsx')
periods = pd.read_excel(r'.\codes\periods.xlsx', sheet_name ='period_lbl')
time_period_type = pd.read_excel(r'.\codes\periods.xlsx', sheet_name ='time_period_type')
year = pd.read_excel(r'.\codes\periods.xlsx', sheet_name ='year')
segments_order = pd.read_excel(r'.\codes\segments_order.xlsx')
example = pd.read_excel(r'.\codes\Example.xlsx', sheet_name = 'example')
legends = pd.read_excel(r'.\codes\Example.xlsx', sheet_name = 'legends')
shop_order = pd.read_excel(r'.\codes\shop_order.xlsx')

fin_cols = example.columns

cat_dict = {
    'br':'MALE B&R', 'deterg': 'Laundry Detergents',
    'diapers': 'Diapers','femcare': 'Feminine Care', 
    'haircond': 'Hair Conditioners', 'shampoo':'Shampoos'}

columns_dict = legends[['mask','Colum']].dropna(subset='mask').set_index('mask').to_dict()['Colum']

# %% make_functions

def get_period_lbl(time_period_type, year, month):
    return f"{time_period_type.split('/')[0]}: {calendar.month_abbr[month]} {year}"

def add_period_lbls(df_in):

    time_period_type_dict = {24 :'2MATs/ 104 we', 12:'MAT/ 52 we', 3:'3MMT/ 12we', 1: 'Month'}
    
    col_to_int = ['duration', 'year', 'month']
    df_in[col_to_int] = df_in[col_to_int].astype(int)
    
    df_in['time_period_type'] = df_in['duration'].map(time_period_type_dict)
    
    df_tmp_per = df_in[['time_period_type', 'year', 'month']].drop_duplicates()
    df_tmp_per['period_lbl'] = df_tmp_per[['time_period_type', 'year', 'month']]\
        .apply(lambda row: get_period_lbl(*row), axis=1)
        
    df_in = df_in.merge(df_tmp_per, how='left')
        
    print('time_period_type is null:', sum(df_in['time_period_type'].isnull()))   

    return df_in

def get_df_in_v2(df_in, category, fin_cols, columns_dict):
    
    print(category)

    df_in = df_in.rename(
        {'Category Name':'Product Name', 'Buyer Group Name':'buyers_gr_label'}, 
        axis=1)
    df_in['Category Name'] = category
    
    print('init shape', df_in.shape)
        
    # products
    df_in = df_in.merge(
        features_group.drop(columns='Product Name'), 
        left_on=['Category Name', 'Product Name'], 
        right_on=['Category Name', 'full_label'], 
        how='left', validate='many_to_one')
    df_in = df_in.rename(
        columns={'product_code': 'products', 'category_code': 'category', 'feature_code': 'features'}
    )
    df_in['product_hier'] = df_in['products']

    # socdem
    df_in = df_in.merge(
        demo_order, on=['buyers_gr_label'], how='left', validate='many_to_one')
    df_in = df_in.rename(columns={'demo_code': 'demo_groups'})
    df_in['demo_hier'] = df_in['demo_groups']
    
    # periods
    df_in = df_in.merge(
        periods, on=['period_lbl'], how='left', validate='many_to_one')
    df_in = df_in.merge(
        time_period_type, on=['time_period_type'], how='left', validate='many_to_one')
    df_in = df_in.drop(columns=['time_period_type', 'period_lbl'])\
        .rename(columns={'time_period_code': 'time_period_type', 'period_code': 'period_lbl'})
    
    # df_in = df_in.merge(
    #     year, on=['year'], how='left', validate='many_to_one')
    df_in['year'] = df_in['year'].astype(int)
    
    # segments
    df_in = df_in.merge(
        segments_order, on=['Segment'], how='left', validate='many_to_one')
    df_in = df_in.rename(columns={'segment_code': 'cat_segment'})
    
    print('merged shape', df_in.shape)
    
    
    # shop
    df_in = df_in.merge(
        shop_order, on=['position_name_shop'], how='left', validate='many_to_one')
    
    
    # rename metrics
    df_in.columns = [col.lower().replace(' ','_') for col in df_in.columns]
    df_in = df_in.rename(columns_dict, axis=1)
    df_in = df_in[[i for i in fin_cols if i in df_in.columns]].copy()

    chk_na = df_in.isna().sum()
    if chk_na[chk_na > 0].shape[0] > 0:
        print('WARNING\n', 'columns with na:')
        print(chk_na[chk_na > 0])
        
    # transform from 000
    # TODO: add usd when avaliable
    cols_to_transform = [
        'buying_households', 
        'occasions', 
        'volume_physical_units',
        'volume_su',
        'spend_local_currency']
    df_in[cols_to_transform] = df_in[cols_to_transform] * 1000
    
    return df_in

#%% load data file with name 'out-for-datatile.7z' 

# archive = py7zr.SevenZipFile('../data_in/out-for-datatile.7z', 'r')
# archive.extractall(path="/tmp")

path = './data/out-for-datatile'
# files = os.listdir(path)

# only total
files = list(Path(path).glob('*_tot.parquet'))
# # with shops
# files = list(Path(path).glob('*_allshops.parquet'))

# data check
dfs = []
for f in files:
    category = cat_dict[f.name.split('_')[0]]
    df_in = pd.read_parquet(f)
    df_in['category'] = category

    dfs.append(df_in)
    
df_check = add_period_lbls(pd.concat(dfs))
df_check = df_check.merge(periods, how='left', validate='many_to_one')
new_periods = df_check.loc[df_check['period_code'].isna(), 'period_lbl'].unique()
if len(new_periods):
    print('new periods:')
    print(df_check.loc[df_check['period_code'].isna(), 'period_lbl'].unique())
    # TODO: save to excel in codes dir
    
# TODO: check for retailers in categories




# TODO: save to excel in codes dir
# TODO: create excel with shops / hierarchy and common codes for all cats

duplicates_n = df_check.loc[df_check.duplicated()].shape[0]
if duplicates_n:
    print(duplicates_n, 'duplicates found')

# %% process data by categories
dfs = []
for f in files:
    
    category = cat_dict[f.name.split('_')[0]]
    df_in = pd.read_parquet(f)
    
    df_in = add_period_lbls(df_in)
    
    if ('Segment' not in df_in.columns):
        if (category == 'Feminine Care'):
            df_in['Segment'] = 'Total Feminine Care'
        if (category == 'Shampoos'):
            df_in['Segment'] = 'Shampoos'
    
    if (category == 'Laundry Detergents'):
        df_in['Segment'] = df_in['Segment'].replace(
            {'Total Detergents excluding Bars': 'Total Detergents excluding Bar'})
    
    df_in_one = get_df_in_v2(df_in, category, fin_cols, columns_dict)
    
    print('missing cols', [i for i in fin_cols if i not in df_in_one.columns], '\n')
    
    dfs.append(df_in_one)
    
    del df_in_one
    
df = pd.concat(dfs)
# print(df.columns)

# %% drop duplicates
i = df.shape[0]
df[[i for i in columns_dict.values() if i in df.columns]] = \
    df[[i for i in columns_dict.values() if i in df.columns]].round(6)
df = df.drop_duplicates()
print(i - df.shape[0], 'duplicates dropped')

# %% save output
print('saving output')
df.to_parquet('data/new_data_merged/new_data.pq', index=False)
