import pandas as pd
import calendar
import py7zr
import numpy as np
from pathlib import Path
from zipfile import ZipFile


# %% load dicts
features_group = pd.read_excel(r'.\codes\feature_groups.xlsx')
demo_order = pd.read_excel(r'.\codes\demo_order.xlsx')
demo_order['buyers_gr_label'] = demo_order['buyers_gr_label'].astype(str).str.lower()
periods = pd.read_excel(r'.\codes\periods.xlsx', sheet_name ='period_lbl')
time_period_type = pd.read_excel(r'.\codes\periods.xlsx', sheet_name ='time_period_type')
year = pd.read_excel(r'.\codes\periods.xlsx', sheet_name ='year')
segments_order = pd.read_excel(r'.\codes\segments_order.xlsx')
example = pd.read_excel(r'.\codes\Example.xlsx', sheet_name = 'example')
legends = pd.read_excel(r'.\codes\Example.xlsx', sheet_name = 'legends')
shop_order = pd.read_excel(r'.\codes\shop_order.xlsx')

fin_cols = example.columns.to_list() + ['shop_code', 'shop_lvls','channel_code', 'file']




def get_category(name_of_file):
    cat_dict = {
    'br':'MALE B&R', 'deterg': 'Laundry Detergents',
    'diapers': 'Diapers','femcare': 'Feminine Care', 
    'haircond': 'Hair Conditioners', 'shampoo':'Shampoos'}
    for k in cat_dict.keys():
        if name_of_file.count(k):
            return cat_dict[k]
    else:
        raise ValueError('В названии файла нет категории', name_of_file)    

columns_dict = legends[['mask','Colum']].dropna(subset='mask').set_index('mask').to_dict()['Colum']

# %% make_functions

def get_period_lbl(time_period_type, year, month):
    return f"{time_period_type.split('/')[0]}: {calendar.month_abbr[month]} {year}"

def get_label_num(time_period_type, year, month):
    return f"{time_period_type.split('/')[0]}: {year} ({str(month).zfill(2)}) {calendar.month_abbr[month]}"



def add_period_lbls(df_in, periods,time_period_type):


    time_period_type_dict = {24 :'2MATs/ 104 we', 12:'MAT/ 52 we', 3:'3MMT/ 12we', 1: 'Month'}
    
    col_to_int = ['duration', 'year', 'month']
    df_in[col_to_int] = df_in[col_to_int].astype(int)
    
    df_in['time_period_type'] = df_in['duration'].map(time_period_type_dict)
    
    df_tmp_per = df_in[['time_period_type', 'year', 'month']].drop_duplicates()
    df_tmp_per['period_lbl'] = df_tmp_per[['time_period_type', 'year', 'month']]\
        .apply(lambda row: get_period_lbl(*row), axis=1)
    
    df_tmp_per['label_num'] = df_tmp_per[['time_period_type', 'year', 'month']]\
        .apply(lambda row: get_label_num(*row), axis=1)
    


    new_periods = set(df_tmp_per['period_lbl']) - set(periods['period_lbl'])
    if len(new_periods):
        print('New periods!')
        print(*new_periods)
        new_periods_df = df_tmp_per[df_tmp_per['period_lbl'].isin(new_periods)].sort_values(by=['year','month'])
        new_periods_df['period_batch'] = periods['period_batch'].max()+1 if ~np.isnan(periods['period_batch'].max()) else 0
        periods = pd.concat([periods[['period_lbl','label_num','period_batch' ]], new_periods_df[['period_lbl','label_num','period_batch' ]]],ignore_index=True )
        periods['period_code'] = range(1,len(periods)+1)
        #periods.index = periods['period_code']
        #periods = periods.drop('period_code',axis =1) 

        with pd.ExcelWriter(r'.\codes\periods.xlsx',engine="openpyxl",mode="a", if_sheet_exists='replace') as writer:
            periods.to_excel(writer, sheet_name ='period_lbl')
            #time_period_type.to_excel(writer, sheet_name ='time_period_type')




    df_in = df_in.merge(df_tmp_per, how='left')
    
    df_in = df_in.merge(
            periods, on='period_lbl', how='left', validate='many_to_one')
    df_in = df_in.merge(
            time_period_type, on=['time_period_type'], how='left', validate='many_to_one')

    df_in = df_in.drop(columns=['time_period_type', 'period_lbl', 'month' ])\
            .rename(columns={'time_period_code': 'time_period_type', 'period_code': 'period_lbl'})

    return df_in

def get_df_in_v2(df_in, category,  fin_cols, columns_dict):
    


    df_in = df_in.rename(
        {'Category Name':'Product Name', 'Buyer Group Name':'buyers_gr_label'}, 
        axis=1)
    df_in['Category Name'] = category
    
    #print('init shape', df_in.shape)


        
    # products
    prod_col_to_merge = 'Product Name'
    if  len(set(df_in['Product Name'].unique()) - set(features_group['Product Name']))>0:
        print('Мерджим по фулл нейму')
        prod_col_to_merge = 'full_label'

    
    df_in = df_in.merge(
        features_group,#.drop(columns='Product Name'), 
        left_on=['Category Name', 'Product Name'], 
        right_on=['Category Name',  prod_col_to_merge], 
        how='left') #, validate='many_to_one') Есть множественные ключи в features_group['Product Name']
    




    df_in = df_in.rename(
        columns={'product_code': 'products', 'category_code': 'category', 'feature_code': 'features'}
    )
    df_in['product_hier'] = df_in['products']


    # socdem
    df_in['buyers_gr_label'] = df_in['buyers_gr_label'].str.lower()
    df_in = df_in.merge(
        demo_order, on=['buyers_gr_label'], how='left', validate='many_to_one')
    df_in = df_in.rename(columns={'demo_code': 'demo_groups'})
    df_in['demo_hier'] = df_in['demo_groups']

    
    # segments

    if ('Segment' not in df_in.columns):
        if (category == 'Feminine Care'):
            df_in['Segment'] = 'Total Feminine Care'
        elif (category == 'Shampoos'):
            df_in['Segment'] = 'Shampoos'
    

    
    if (category == 'Laundry Detergents'):
        df_in['Segment'] = df_in['Segment'].replace(
            {'Total Detergents excluding Bars': 'Total Detergents excluding Bar'})
    df_in['Segment'] =df_in['Segment'].replace({'Total Diapers size':'Total Diapers'})
    df_in = df_in.merge(
        segments_order, on=['Segment'], how='left', validate='many_to_one')
    df_in = df_in.rename(columns={'segment_code': 'cat_segment'})


    # shop
    df_in['position_name_shop'] = df_in['position_name_shop'].replace(
    {'Ok Hyper': 'OK Hyper', 'Perekryostok': 'Perekrestok'})
    df_in = df_in.merge(
        shop_order, on=['position_name_shop'], how='left', validate='many_to_one')

    
    #print('merged shape', df_in.shape)
    
    # rename metrics
    df_in.columns = [col.lower().replace(' ','_') for col in df_in.columns]
    df_in = df_in.rename(columns_dict, axis=1)
    #df_in = df_in[[i for i in fin_cols + ['duration','month'] if i in df_in.columns]].copy()

    # chk_na = df_in.isna().sum()
    # if chk_na[chk_na > 0].shape[0] > 0:
    #     print('WARNING\n', 'columns with na:')
    #     print(chk_na[chk_na > 0])
        
    # # transform from 000
    # cols_to_transform = [
    #     'buying_households', 
    #     'occasions', 
    #     'volume_physical_units',
    #     'volume_su',
    #     'spend_local_currency']
    # df_in[cols_to_transform] = df_in[cols_to_transform] * 1000
    
    return df_in

#%% 



# files = os.listdir(path)

# # only total
# files = list(Path(path).glob('*_tot.parquet'))
# with shops



# files = []
# for path in paths:
#     files += list(Path(path).glob('*_allshops.parquet'))

# # data check
# dfs = []
# for f in files:
#     category = cat_dict[f.name.split('_')[0]]
#     df_in = pd.read_parquet(f)
#     df_in['category'] = category

#     dfs.append(df_in)

#load data from dir 'new_format_data

paths = Path('data/new_format_data')

zip_files = list(paths.glob('*.zip')) 
tmp_zip = Path('./data/tmp/7z')

z_files = list(paths.glob('*.7z'))
tmp_7z = Path('./data/tmp/7z')

anti_pattern = '_tot.parquet'
parket = '.parquet'
dfs = []
for zip_f in zip_files:
    with ZipFile(zip_f) as zip_file: 
        info = zip_file.namelist() 
        for name in info:
            if (name.count(anti_pattern) == 0) and name.count(parket):
                print(name)
                category = get_category(name)
                tmp_df  = pd.read_parquet(zip_file.extract(name,path=tmp_zip))
                tmp_df = get_df_in_v2(tmp_df, category,  fin_cols, columns_dict)
                tmp_df['file'] = name                   
                dfs.append(tmp_df)
 

for z in z_files:
    with py7zr.SevenZipFile(z, 'r') as archive:
        info = archive.namelist() 
        names = [name for name in info if ( (name.count(anti_pattern) == 0) and name.count(parket))>0 ]
        archive.reset()
        archive.extract(path = tmp_7z, targets=names)
        for name in names:
            print(name)
            category = get_category(name)
            tmp_df = pd.read_parquet(tmp_7z.joinpath(name)) 
            tmp_df = get_df_in_v2(tmp_df, category,  fin_cols, columns_dict)
            tmp_df['file'] = name   
            dfs.append(tmp_df)


print('Всего файлов', len(dfs))
# Add periods    

df_check = pd.concat(dfs, ignore_index=True)
df_check.reset_index(drop=True)
print('Before periods',df_check.shape)
df_check = add_period_lbls(df_check, periods,time_period_type)
print('After periods',df_check.shape)


#Chek na


chk_na = df_check.isna().sum()
if chk_na[chk_na > 0].shape[0] > 0:
    print('WARNING\n', 'columns with na:')
    print(chk_na[chk_na > 0])    


#df_check = get_df_in_v2(df_check, fin_cols, columns_dict)

print('missing cols', [i for i in fin_cols if i not in df_check.columns], '\n')
#drop not nessesery columns
df_check = df_check.drop(list(set(df_check.columns) - set(fin_cols)), axis=1)
# print(df.columns)

# %% drop duplicates
i = df_check.shape[0]
df_check[[i for i in columns_dict.values() if i in df_check.columns]] = \
    df_check[[i for i in columns_dict.values() if i in df_check.columns]].round(3)
df_check = df_check.drop_duplicates()
print(i - df_check.shape[0], 'duplicates dropped')



# %% save output
print('saving output')
df_check.to_parquet('data/new_data_merged/new_data.pq', index=False)
