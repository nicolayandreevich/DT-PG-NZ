import pandas as pd
from calendar import month_abbr, different_locale
#import py7zr
from pathlib import Path
import datetime
from tqdm import tqdm

print('Начали ',datetime.datetime.now().strftime('%HH-%MM-%SS'))

# %% load dicts
features_group = pd.read_excel(r'.\codes\feature_groups.xlsx')
demo_order = pd.read_excel(r'.\codes\demo_order.xlsx')
demo_order['buyers_gr_label'] = demo_order['buyers_gr_label'].astype(str).str.lower()
segments_order = pd.read_excel(r'.\codes\segments_order.xlsx')
example = pd.read_excel(r'.\codes\Example.xlsx', sheet_name = 'example')
legends = pd.read_excel(r'.\codes\Example.xlsx', sheet_name = 'legends')
shop_order = pd.read_excel(r'.\codes\shop_order.xlsx')
today = datetime.datetime.now().strftime('%Y-%m-%d')


list_of_category = ['Hair Conditioners',
 'Laundry Detergents',
 'MALE B&R',
 'Shampoos',
 'Feminine Care',
 'Diapers']

#get_low!
list_of_dict = [features_group,demo_order,
                 segments_order,example,legends,shop_order]
for d in list_of_dict:
    d.columns = [c.lower() for c in d.columns ]

fin_cols = example.columns.to_list() + ['shop_code', 'shop_lvls','channel_code', 'file',]
columns_dict = legends[['mask','Colum'.lower()]].dropna(subset='mask').set_index('mask').to_dict()['Colum'.lower()]



def get_category(name_of_file):
    cat_dict = {'br': 'MALE B&R',
 'deterg': 'Laundry Detergents',
 'diapers': 'Diapers',
 'femcare': 'Feminine Care',
 'haircond': 'Hair Conditioners',
 'shampoo': 'Shampoos',
 'MALE B&R': 'MALE B&R',
 'Laundry Detergents': 'Laundry Detergents',
 'Diapers': 'Diapers',
 'Feminine Care': 'Feminine Care',
 'Hair Conditioners': 'Hair Conditioners',
 'Shampoos': 'Shampoos'}
    for k in cat_dict.keys():
        if name_of_file.count(k):
            return cat_dict[k]
    else:
        raise ValueError('В названии файла нет категории', name_of_file)    


# %% make_functions

def get_period_lbl(time_period_type, year, month):
    with different_locale('English'):
        return f"{time_period_type.split('/')[0]}: {month_abbr[month]} {year}"

def get_label_num(time_period_type, year, month):
    with different_locale('English'):
        return f"{time_period_type.split('/')[0]}: {year} ({str(month).zfill(2)}) {month_abbr[month]}"


def add_period_lbls(df_in):
    periods = pd.read_excel(r'.\codes\periods.xlsx', sheet_name ='period_lbl').drop(columns='Unnamed: 0')
    time_period_type = pd.read_excel(r'.\codes\periods.xlsx', sheet_name ='time_period_type')


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
        new_periods_df['date_added'] = today
        periods = pd.concat([periods[['period_lbl','label_num','date_added' ]], new_periods_df[['period_lbl','label_num','date_added' ]]],ignore_index=True )
        periods['period_code'] = range(1,len(periods)+1)


        with pd.ExcelWriter(r'.\codes\periods.xlsx',engine="openpyxl",mode="a", if_sheet_exists='replace') as writer:
            periods.to_excel(writer, sheet_name ='period_lbl')





    df_in = df_in.merge(df_tmp_per,on = ['time_period_type', 'year', 'month'], how='left')
    
    df_in = df_in.merge(
            periods, on=['period_lbl', 'label_num'],  how='left', validate='many_to_one')
    
    
    
    df_in = df_in.merge(
           time_period_type, on='time_period_type', how='left', validate='many_to_one')

    df_in = df_in.drop(columns=['time_period_type', 'period_lbl', 'month' ])\
            .rename(columns={'time_period_code': 'time_period_type', 'period_code': 'period_lbl'})

    return df_in

def get_df_in_v2(df_in, category, columns_dict):
    
    #get_low_2!
    df_in.columns = [c.lower() for c in df_in.columns]
    income_columns = list(df_in.columns)


    df_in = df_in.rename(
        {'Category Name'.lower():'Product Name'.lower(), 'Buyer Group Name'.lower():'buyers_gr_label'}, 
        axis=1)
    df_in['Category Name'.lower()] = category
    df_in['buyers_gr_label'] = df_in['buyers_gr_label'].replace({'Total Population' :'TOTAL DEMOGRAPHICS'})
    df_in = df_in.drop('category',axis=1)


    #print('init shape', df_in.shape)


        
    # products
    #prod_col_to_merge = 'Product Name'.lower()
    #if  len(set(df_in['Product Name'.lower()].unique()) - set(features_group['Product Name'.lower()]))>0:
    #    print('Мерджим по фулл нейму')
    #    prod_col_to_merge = 'full_label'
    #    features_group = features_group.drop(columns='Product Name'.lower())
    prod_col_to_merge = 'full_label'

    
    df_in = df_in.merge(
        features_group, 
        left_on=['Category Name'.lower(), 'Product Name'.lower()], 
        right_on=['Category Name'.lower(),  prod_col_to_merge], 
        how='left', validate='many_to_one') #Есть множественные ключи в features_group['Product Name']
    




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

    if ('Segment'.lower() not in df_in.columns):
        if (category == 'Feminine Care'):
            df_in['Segment'.lower()] = 'Total Feminine Care'
        elif (category == 'Shampoos'):
            df_in['Segment'.lower()] = 'Shampoos'
    

    
    if (category == 'Laundry Detergents'):
        df_in['Segment'.lower()] = df_in['Segment'.lower()].replace(
            {'Total Detergents excluding Bars': 'Total Detergents excluding Bar'})
    df_in['Segment'.lower()] =df_in['Segment'.lower()].replace({'Total Diapers size':'Total Diapers'})
    df_in = df_in.merge(
        segments_order, on=['Segment'.lower()], how='left', validate='many_to_one')
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
    df_in = df_in[[i for i in df_in.columns if i in fin_cols + ['duration','month'] + income_columns ]]
    
    return df_in
    
def get_buyers_shares(df_in): # %% value / buyers shares for socdem
    cols_to_merge = list(df_in.columns[df_in.dtypes != 'float64'].difference([*demo_order.columns, 'demo_groups',
                                                                              'date_added']))

    cols_to_rename = {'spend_local_currency': 'total_spend_local_currency', 
                'buying_households': 'total_buying_households'}
        
    totals = df_in.loc[df_in['demo_groups'] == 1,[*cols_to_merge,'spend_local_currency','buying_households']].rename(columns=cols_to_rename)
    totals = totals.drop_duplicates()
    df_in = df_in.merge(totals, how='left',  validate='many_to_one')

    df_in['value_share'] = df_in['spend_local_currency'] / df_in['total_spend_local_currency'] * 100
    df_in['buyers_share'] = df_in['buying_households'] / df_in['total_buying_households'] * 100


    df_in = df_in.drop(columns=['total_spend_local_currency', 'total_buying_households'])
    
    return df_in

paths = Path('data/new_format_data')

zip_files = list(paths.glob('*.zip')) 
tmp_zip = Path('./data/tmp/zip').mkdir(parents=True, exist_ok=True)

z_files = list(paths.glob('*.7z'))
tmp_7z = Path('./data/tmp/7z').mkdir(parents=True, exist_ok=True)


#todo add unzip 7zip 


files = list(Path('./data/tmp/in').rglob('*.parquet') )
categories = list([get_category(name.name) for name in files])
file_df = pd.DataFrame(files,categories).reset_index()
file_df.columns = ['category', 'file']

for c in  file_df['category'].unique():
    print(c)
    names = file_df[file_df['category']==c]['file'].values
    categry_df = pd.DataFrame()
    progress_bar = tqdm(names)
    for f in progress_bar:
        progress_bar.set_postfix({'name_of_file': f})
        tmp_df = pd.read_parquet(f) 
        tmp_df['file'] = '/'.join(f.parts[-2:])
        categry_df = pd.concat([categry_df,tmp_df ], ignore_index= True )

    categry_df = get_df_in_v2(categry_df, c,columns_dict)
    categry_df = add_period_lbls(categry_df)
    metric_cols = list(categry_df.select_dtypes(include='float').columns)
    categry_df = categry_df[[col for col in set(fin_cols + metric_cols) if col in categry_df.columns  ]]
    chk_na = categry_df.isna().sum()
    if chk_na[chk_na > 0].shape[0] > 0:
        print('WARNING\n', 'columns with na:')
        print(chk_na[chk_na > 0])          
    i= categry_df.shape[0]
    categry_df[[i for i in metric_cols if i in categry_df.columns]] = \
    categry_df[[i for i in metric_cols if i in categry_df.columns]].round(3)
    categry_df = categry_df.drop_duplicates(subset=categry_df.columns.difference(['file'])) #Смотрим на файл при удалении дубликатов?
    print(i - categry_df.shape[0], 'duplicates dropped')
    categry_df = get_buyers_shares(categry_df)
    print('missing cols', [i for i in fin_cols if i not in categry_df.columns], '\n')  
    metric_cols = list(categry_df.select_dtypes(include='float').columns)   
    categry_df.to_parquet(f'data/tmp/{c}.pq', index=False)
    

print('Готово ',datetime.datetime.now().strftime('%HH-%MM-%SS'))
