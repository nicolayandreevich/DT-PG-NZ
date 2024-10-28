import pandas as pd
from tqdm import tqdm
from RM_tools import dp_utils

chunk_size = 1_000_000

files = [
    'PG_Flatfiles_Diapers',
    'PG_Flatfiles_BRMALE',
    'PG_Flatfiles_Shampoo',
    'PG_Flatfiles_Feminine_Care',
    'PG_Flatfiles_Hair_Conditioners',
    'PG_Flatfiles_Laundry_Detergents',
]

# %% load old format data
dfs = []
for f in tqdm(files):
    # df_tmp = pd.read_csv(f'exports/{f}.zip', engine='pyarrow')
    df_tmp = pd.read_parquet(f'exports/{f}.pq.zstd', engine='pyarrow')
    dfs.append(df_tmp)

df = pd.concat(dfs)
del df_tmp, dfs

# %% add labels
product_order = pd.read_excel('codes/feature_groups.xlsx')
demo_order = pd.read_excel('codes/demo_order.xlsx')
segment_order = pd.read_excel('codes/segments_order.xlsx')

def dic_inv(dic):
    return {v: k for k, v in dic.items()}

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

product_new_codes = dict(zip(product_order['Vendor Product ID'], product_order['product_code']))
lbl_dic['products'] = dict(zip(product_order['product_code'], product_order['full_label']))

demo_new_codes = dict(zip(demo_order['Buyer Group ID'], demo_order['demo_code']))
lbl_dic['demo_groups'] = dict(zip(demo_order['demo_code'], demo_order['buyers_gr_label']))

df['product_lvls']= df['product_lvls'].astype(int)
lbl_dic['product_lvls'] = {k: str(k) for k in df['product_lvls'].unique()}

df['demo_lvls']= df['demo_lvls'].astype(int)
lbl_dic['demo_lvls'] = {k: str(k) for k in df['demo_lvls'].unique()}

# %% recode and add order to levels

# products
df['products'] = df['Vendor Product ID'].map(product_new_codes)
print('products w/o label: ', df['products'].isna().sum())
df['features'] = df['feature'].map(dic_inv(lbl_dic['features']))
print('features w/o label: ',df['features'].isna().sum())
df['cat_segment'] = df['Segment'].map(dic_inv(lbl_dic['cat_segment']))
print('segments w/o label: ',df['cat_segment'].isna().sum())

df['category'] = df['category'].map(cat_new_codes)
print('categories w/o label: ',df['category'].isna().sum())

# lbl_dic['product_hier'] = dict(zip(df['products'], df['product_hier']))
lbl_dic['product_hier'] = dict(zip(product_order['product_code'], product_order['product_hier']))
df['product_hier'] = df['products']

df['socdem_gr'] = df['socdem_gr_code'].map(demo_new_codes)
print('socdem groups w/o label: ',df['socdem_gr'].isna().sum())
lbl_dic['socdem_gr'] = {k:v for k, v in lbl_dic['demo_groups'].items() if k in df['socdem_gr'].unique()}

df['demo_groups'] = df['Buyer Group ID'].map(demo_new_codes)
print('demo groups w/o label: ',df['demo_groups'].isna().sum())
# lbl_dic['demo_hier'] = dict(zip(df['demo_groups'], df['demo_hier']))
lbl_dic['demo_hier'] = dict(zip(demo_order['demo_code'], demo_order['demo_hier']))
df['demo_hier'] = df['demo_groups']

df = df.drop(columns=['socdem_gr_code', 'category_code'])

# df['level_0'] = df['level_0'].map(cat_new_codes)
# df = df.drop(columns='level_0_code')

# for i in range(1, 8):
#     df['level_' + str(i) + '_code'] = df['level_' + str(i) + '_code'].map(product_new_codes)
#     lbl_dic['level_'+str(i)] = dict(
#         zip(
#             df['level_' + str(i) + '_code'].dropna().values, 
#             df['level_' + str(i)].dropna().values
#         )
#     )
 
# df = df.drop(columns=['level_'+str(i) for i in range(1, 8)])\
#     .rename(columns={'level_' + str(i) + '_code': 'level_'+str(i) for i in range(1, 8)})

# # demo
# df['demo_groups'] = df['Buyer Group ID'].map(demo_new_codes)
# df = df.drop(columns=['demo_0', 'demo_0_code'])
# for i in range(1, 5):
#     df['demo_' + str(i) + '_code'] = df['demo_' + str(i) + '_code'].map(demo_new_codes)
#     lbl_dic['demo_'+str(i)] = dict(
#         zip(
#             df['demo_' + str(i) + '_code'].dropna().values, 
#             df['demo_' + str(i)].dropna().values
#         )
#     )

# df = df.drop(columns=['demo_' + str(i) for i in range(1, 5)])\
#     .rename(columns={'demo_' + str(i) + '_code': 'demo_' + str(i) for i in range(1, 5)})

# %% drop cols
df = df.drop(columns=[
    'Vendor Product ID', 'Product Name', 'feature', 
    'Segment', 'Buyer Group ID', 'Buyer Group Name',
    'Projected Shoppers', 'Panel Sample Size'
    ])

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
    'Volume SU': 'Volume SU',
    'Volume Physical Units': 'Volume Packs',
    'Spend Local Currency': 'Value RUB',
    # 'Spend EUR': 'Value EUR',
    'Spend USD': 'Value USD',    
}

metrics_dic = {
    k.lower().replace(' ', '_').replace('+', '_'): v for k, v in metrics.items()
}

df.columns = [k.lower().replace(' ', '_').replace('+', '_') for k in df.columns]

# %% encode periods
period_dic = pd.read_excel('codes/periods.xlsx', sheet_name=None)

df['year'] = df['year'].astype(int)
# lbl_dic['year'] = {k: str(k) for k in df['year'].unique()}
lbl_dic['year'] = dict(zip(period_dic['year']['year_code'], period_dic['year']['year']))
# lbl_dic['time_period_type'] = {1: '3MMT/ 12we', 2:'MAT/ 52 we', 3: '2MAT/ 104 we', 4: 'Monthly'}
lbl_dic['time_period_type'] = dict(zip(
    period_dic['time_period_type']['time_period_code'], 
    period_dic['time_period_type']['time_period_type']))
df['time_period_type'] = df['time_period_type'].replace(
    {'Monthly': 'Month', '2MAT/ 104 we': '2MATs/ 104 we'})
df['time_period_type'] = df['time_period_type'].map(dic_inv(lbl_dic['time_period_type']))
print('period types w/o label: ', df['time_period_type'].isna().sum())

# lbl_dic['period_lbl'] = {k: v for k, v in enumerate(sorted(df['period_lbl'].unique().tolist()), 1)}
lbl_dic['period_lbl'] = dict(zip(period_dic['period_lbl']['period_code'], period_dic['period_lbl']['label_num']))
df['period_lbl'] = df['period_lbl'].map(dic_inv(lbl_dic['period_lbl']))
# for k, v in lbl_dic['period_lbl'].items():
#     spl =  v.split(' ')
#     lbl_dic['period_lbl'][k] = spl[0] + ' ' + spl[3] + ' ' + spl[1]
lbl_dic['period_lbl'] = dict(zip(period_dic['period_lbl']['period_code'], period_dic['period_lbl']['period_lbl']))
print('periods w/o label: ', df['period_lbl'].isna().sum())

# %% get currency rates
# df['rate'] = (df['spend_local_currency'] / df['spend_usd']).round(2)
# rates = df[['period_lbl', 'rate']].drop_duplicates().set_index('period_lbl').to_dict()['rate']

# %% add new format data

# upload new data
df_new = pd.read_parquet('data/new_data_merged/new_data.pq')

# replace incorrect su
incorrect_su_old = [
    'average_per_su_local_currency',
    'average_per_su_usd',
    'purchase_size_su',
    'volume_su'
]
df[incorrect_su_old] = float('nan')
cols = df.columns
df = df.drop(columns=[i for i in incorrect_su_old if i in df_new.columns])
df = df.merge(
    df_new[[i for i in df_new.columns if (i not in metrics_dic.keys()) or (i in incorrect_su_old)]].drop_duplicates(), 
    how='left', validate='one_to_many')

# concat, keep first data
df_union = pd.concat([df, df_new], ignore_index=True)
df_union = df_union.drop_duplicates(
    subset=[i for i in df_union.columns if i not in metrics_dic.keys()], keep='first')

# %% value / buyers shares for socdem
cols = ['product_lvls', 'category',
        'time_period_type', 'year', 'period_lbl',
        'product_hier', 'products', 
        'features', 'cat_segment',
        'spend_local_currency', 'buying_households'
        ]
totals = df_union.loc[df_union['demo_groups'] == 1, cols].rename(
    columns={'spend_local_currency': 'total_spend_local_currency', 
             'buying_households': 'total_buying_households'})
totals = totals.drop_duplicates()

df_union = df_union.merge(totals, how='left', validate='many_to_one')

df_union['value_share'] = df_union['spend_local_currency'] / df_union['total_spend_local_currency'] * 100
df_union['buyers_share'] = df_union['buying_households'] / df_union['total_buying_households'] * 100

metrics_dic.update({
    'value_share': 'Value share, % of Total Demography',
    'buyers_share': 'Buyers share, % of Total Demography'
    })

df_union = df_union.drop(columns=['total_spend_local_currency', 'total_buying_households'])
del totals

# %% prepare sav file # SavReaderWriter
varNames = [i.lower().encode() for i in df_union.columns]        
varTypes = dict.fromkeys(varNames, 0)
formats = {c.encode(): b'F8.5' for c in metrics_dic.keys()}

varLabels = {}
cols = {key.encode(): val.encode() for key, val in metrics_dic.items()}
varLabels = {
    key: cols[key]
    if key in cols.keys() else key for key in varNames}

valueLabels = {
    key.lower().encode(): val 
    for key, val in lbl_dic.items() 
    if key.lower().encode() in varNames}
for key in valueLabels.keys():
    valueLabels[key] = {k: str(i).encode() for k, i in valueLabels[key].items()}    

for c in df_union['category'].unique():
    df_tmp = df_union[df_union['category'] == c].copy()
    df_tmp.index = list(range(df_tmp.shape[0]))
    for i in range(df_tmp.shape[0] // chunk_size + 1):
        dp_utils.save_convert(
                f'PG_cat{c}_chunk{i}', 
                df_tmp[
                    (df_tmp['category'] == c) 
                    & (df_tmp.index >= i * chunk_size) 
                    & (df_tmp.index < (i+1) * chunk_size)
                ],
                varNames, varTypes, valueLabels, 
                varLabels, formats, dir='sav_conv') 

