#%%
import pandas as pd
from tqdm import tqdm
from RM_tools import dp_utils
from pathlib import Path
import pickle 
chunk_size = 1_000_000


# %% load old format data

files = Path('./data/tmp').glob('*.pq')
dfs = []
for file in files:
    print(file.stem)
    tmp = pd.read_parquet(file)
    #tmp = tmp.drop(columns = ['label_num','segment', 'file', 'duration']) #Нужен этот столбец? Дубль period_lbl
    print(tmp.shape)
    dfs.append(tmp)
    


df_union = pd.concat(dfs, ignore_index=True)

del dfs

print("Creating united data file complite!",df_union.shape )


# %% value / buyers shares for socdem
# cols = ['product_lvls', 'category',
#         'shop_code', 'shop_lvls',
#         'time_period_type', 'year', 'period_lbl',
#         'product_hier', 'products', 
#         'features', 'cat_segment','demo_hier'] 
# cols_to_rename = {'spend_local_currency': 'total_spend_local_currency', 
#              'buying_households': 'total_buying_households'}
        
# totals = df_union.loc[df_union['demo_groups'] == 1, cols+ [*cols_to_rename.keys()]].rename(
#     columns={'spend_local_currency': 'total_spend_local_currency', 
#              'buying_households': 'total_buying_households'})
# totals = totals.drop_duplicates()

# df_union = df_union.merge(totals, how='left',left_on =cols + [*cols_to_rename.keys()], right_on =cols +[*cols_to_rename.values()],  validate='many_to_one')

# df_union['value_share'] = df_union['spend_local_currency'] / df_union['total_spend_local_currency'] * 100
# df_union['buyers_share'] = df_union['buying_households'] / df_union['total_buying_households'] * 100



#df_union = df_union.drop(columns=['total_spend_local_currency', 'total_buying_households'])
#del totals


# %% variable labels
metrics = {
    'Buying Households': 'Buyers (000 HH)',
    # 'Projected Shoppers': 'Buyers (000 HH)',
    # 'Panel Sample Size': 'Population Raw',
    'Raw Shoppers': 'Raw Buyers',
    'Loyalty Volume Based': 'Loyalty Volume',
    'Loyalty Value Based Local Currency': 'Loyalty Value RUB',
    # 'Loyalty Value Based EUR': 'Loyalty Value EUR',
    'Loyalty Value Based USD': 'Loyalty Value USD',    
    'Percent 2+ Time Buyers': 'Repeat Rate (percent of buyers with frequency 2 and more)',
    'Percent Household Penetration': 'Penetration',
    'Purchase Frequency': 'Frequency',
    'Occasions': 'Trips (000)',
    'Item Buying Rate local currency': 'Spend per buyer RUB',
    # 'Item Buying Rate EUR': 'Spend per buyer EUR',
    'Item Buying Rate USD': 'Spend per buyer USD',   
    'Purchase size local currency': 'Spend per trip RUB',
    # 'Purchase size EUR': 'Spend per trip EUR',
    'Purchase size USD': 'Spend per trip USD',    
    'Purchase size SU': 'Volume per trip SU',
    'Average per SU local currency': 'Average Price per SU (RUB)',
    # 'Average per SU EUR': 'Average Price L/kg/SU/EUR',
    'Average per SU USD': 'Average Price per SU USD',
    'Volume SU': 'Volume SU (000)',
    'Volume Physical Units': 'Volume Packs (000)',
    'Spend Local Currency': 'Value RUB (000)',
    # 'Spend EUR': 'Value EUR',
    'Spend USD': 'Value USD (000)',    
}

metrics_dic = {
    k.lower().replace(' ', '_').replace('+', '_'): v for k, v in metrics.items()
}

metrics_dic.update({
    'value_share': 'Value share, % of Total Demography',
    'buyers_share': 'Buyers share, % of Total Demography'
    })



# %%
#make lbl_dict
product_order = pd.read_excel('codes/feature_groups.xlsx')
demo_order = pd.read_excel('codes/demo_order.xlsx')
segment_order = pd.read_excel('codes/segments_order.xlsx')
shop_order = pd.read_excel('codes/shop_order.xlsx')
period_dic = pd.read_excel('codes/periods.xlsx', sheet_name=None)


lbl_dic = {}
cat_new_codes = {
    '0. Diapers': 1, 
    '0. MALE B&R': 2, 
    '0. Conditioners': 3,
    '0. Shampoos': 4,
    '0. Total Feminine Care': 5, 
    '0. Laundry Detergents': 6
}
lbl_dic['category'] = dict(zip(product_order['category_code'], product_order['Category Name']))
lbl_dic['level_0'] = lbl_dic['category']

lbl_dic['features'] = dict(zip(product_order['feature_code'], product_order['feature']))

lbl_dic['cat_segment'] = dict(zip(segment_order['segment_code'], segment_order['Segment']))

#product_new_codes = dict(zip(product_order['Vendor Product ID'], product_order['product_code']))
lbl_dic['products'] = dict(zip(product_order['product_code'], product_order['full_label']))

lbl_dic['product_hier'] = dict(zip(product_order['product_code'], product_order['product_hier']))

demo_new_codes = dict(zip(demo_order['Buyer Group ID'], demo_order['demo_code']))
lbl_dic['demo_groups'] = dict(zip(demo_order['demo_code'], demo_order['buyers_gr_label']))


lbl_dic['shop_code'] = dict(zip(shop_order['shop_code'], shop_order['position_name_shop']))
lbl_dic['shop_lvls'] = dict(zip(shop_order['shop_lvls'], shop_order['shop_lvls'].astype(str)))
lbl_dic['channel_code'] = dict(zip(shop_order['channel_code'], shop_order['channel_type']))

lbl_dic['product_lvls'] = {k: str(k) for k in product_order['product_lvls']}
lbl_dic['demo_lvls'] = {k: str(k) for k in demo_order['demo_lvls']}

lbl_dic['year'] = dict(zip(period_dic['year']['year_code'], period_dic['year']['year']))
lbl_dic['time_period_type'] = dict(zip(
    period_dic['time_period_type']['time_period_code'], 
    period_dic['time_period_type']['time_period_type']))

lbl_dic['label_num'] = dict(zip(period_dic['period_lbl']['period_code'], period_dic['period_lbl']['label_num']))

lbl_dic['period_lbl'] = dict(zip(period_dic['period_lbl']['period_code'], period_dic['period_lbl']['period_lbl']))

lbl_dic['socdem_gr'] = dict(zip(demo_order['demo_code'] , demo_order['buyers_gr_label'] ))
lbl_dic['demo_groups'] = dict(zip(demo_order['demo_code'] , demo_order['buyers_gr_label'] ))
lbl_dic['demo_hier'] = dict(zip(demo_order['demo_code'] , demo_order['demo_hier'] ))




print("Колонок без словаря",set(df_union.select_dtypes(exclude='float').columns) - set(metrics_dic.keys()|lbl_dic.keys() ))

values_no_code ={} 
for col  in [key for key in lbl_dic if key in df_union.columns]:
    col_codes = set(lbl_dic[col].keys())
    col_values = set(df_union[col].unique())
    non_code_values_col =  col_values - col_codes
    if len(non_code_values_col):
        values_no_code[col] = non_code_values_col
        print(col)

if len(values_no_code):
    print("Колонок с пропусками кодов", len(values_no_code))
    with open('data/tmp/bad_codes.pickle', 'wb') as f:
        pickle.dump(values_no_code, f)   


#%% meta_information
               
meta_information = df_union[['category','period_lbl', 'demo_groups','shop_code']].drop_duplicates()
meta_information = meta_information.sort_values(by = ['category','period_lbl', 'demo_groups','shop_code']).reset_index(drop=True)


# %% prepare sav file # SavReaderWriter

df_union = df_union.drop('file', axis=1)    


varNames = [i.lower().encode() for i in df_union.columns]        
varTypes = dict.fromkeys(varNames, 0)
formats = {c.encode(): b'F8.5' for c in metrics_dic.keys() if c in df_union.columns } 

varLabels = {}
cols = {key.encode(): val.encode() for key, val in metrics_dic.items() if key in df_union.columns}
varLabels = {
    key: cols[key]
    if key in cols.keys() else key for key in varNames}

valueLabels = {
    key.lower().encode(): val 
    for key, val in lbl_dic.items() 
    if key.lower().encode() in varNames}
for key in valueLabels.keys():
    valueLabels[key] = {k: str(i).encode() for k, i in valueLabels[key].items()} 

#%%Chek of na
for col in df_union.columns:
    if df_union[col].isna().sum():
        print('Колонка с пропуском',col, df_union[col].isna().sum()   )

  


#%% Make sav file
#Chek only_new_mode

def convert_data_to_sav(df_in, 
                        dir='exports/',
                        chunk_size = 1_000_000, 
                        only_new=True ):
    global varNames, varTypes,valueLabels,varLabels,formats, period_dic  
    period_batch = ''
    #if only_new == True:
        #last_period_batch = period_dic['period_lbl']['period_batch'].max()
        #last_period_code = list(period_dic['period_lbl'][period_dic['period_lbl']['period_batch'] == last_period_batch ]['period_code'].values)
        #period_batch = f'and period_lbl in ({last_period_code}) '.replace('[','').replace(']','')   
    for c in (df_in['category'].unique()):
        qu =f"""category == {c} {period_batch}"""
        #print(qu)
        df_tmp = df_in.query(qu).copy() 
        df_tmp = df_tmp.reset_index(drop=True)
        print(c, df_tmp.shape)
        for i in tqdm(range(df_tmp.shape[0] // chunk_size + 1)):
            print(df_tmp.shape[0],i * chunk_size,(i+1) * chunk_size )
            name_of_file = f'PG_cat{c}_chunk{i}' + f"{ f'period_batch_{last_period_batch}' if only_new==True  else ''}"
            with open(f'exports/{name_of_file}.sav','w') as kostil:
                kostil.close()
            dp_utils.save_convert( 
            name_of_file,
                df_tmp[(df_tmp.index >= i * chunk_size) 
                    & (df_tmp.index < (i+1) * chunk_size)
                ],
                varNames, varTypes, valueLabels, 
                varLabels, formats, dir = dir) 


convert_data_to_sav(df_union, only_new= False)
# %%
