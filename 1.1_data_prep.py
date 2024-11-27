import polars as pl

files = [
    'PG_Flatfiles_Diapers',
    'PG_Flatfiles_BRMALE',
    'PG_Flatfiles_Shampoo',
    'PG_Flatfiles_Feminine_Care',
    'PG_Flatfiles_Hair_Conditioners',
    'PG_Flatfiles_Laundry_Detergents',
]

# %% loop for category files
for name in files: 
    
    print(name)
    
    # %% load data
    df = pl.read_parquet(f"data/old_format_data/{name}.pq.zstd")
    df = df.filter(pl.col('Occasions') > 0)
    print(df.shape)
    
    # reduce structure
    if name == 'PG_Flatfiles_Laundry_Detergents':
        df = df.with_columns(
            pl
            .when(pl.col('Vendor Product ID') == 475027)
            .then(250455)
            .otherwise(pl.col('Parent Product ID'))
            .alias('Parent Product ID')
        )
        df = df.filter(~pl.col('Vendor Product ID').is_in([239495, 475025, 475026]))
    
    # %% add periods
    
    df = df.with_columns(
        pl.col('Time Period End Date').cast(str).str.slice(0, 4).alias('year'),
        pl.col('Time Period End Date').cast(str).str.slice(4, 2).alias('month'),
        )
    
    df = df.with_columns(
        pl
        .when(pl.col('month') == "01").then(pl.lit("Jan"))
        .when(pl.col('month') == "02").then(pl.lit("Feb"))
        .when(pl.col('month') == "03").then(pl.lit("Mar"))
        .when(pl.col('month') == "04").then(pl.lit("Apr"))
        .when(pl.col('month') == "05").then(pl.lit("May"))
        .when(pl.col('month') == "06").then(pl.lit("Jun"))
        .when(pl.col('month') == "07").then(pl.lit("Jul"))
        .when(pl.col('month') == "08").then(pl.lit("Aug"))
        .when(pl.col('month') == "09").then(pl.lit("Sep"))
        .when(pl.col('month') == "10").then(pl.lit("Oct"))
        .when(pl.col('month') == "11").then(pl.lit("Nov"))
        .when(pl.col('month') == "12").then(pl.lit("Dec"))
        .otherwise(pl.col('month')).alias('month_name')
    )
    
    df = df.with_columns(
        pl.when(pl.col('Time Period Type') == "MAT/ 52 we").then(
            pl.lit('MAT: ') + pl.col('year') 
            + pl.lit(' (') + pl.col('month') + pl.lit(') ') + pl.col('month_name')
        )
        .when(pl.col('Time Period Type') == "2MAT/ 104 we").then(
            pl.lit('2MATs: ') + pl.col('year') 
            + pl.lit(' (') + pl.col('month') + pl.lit(') ') + pl.col('month_name')
        )
        # .when(pl.col('Time Period Type') == "3MMT/ 12we").then(
        #     pl.lit('QRT: ') + pl.col('year') + pl.lit(' ') 
        #     + (((pl.col('month').cast(int) - 1) / 3).cast(int) + 1).cast(str)
        #     + pl.lit('Q')
        # )
        .when(pl.col('Time Period Type') == "3MMT/ 12we").then(
            pl.lit('3MMT: ') + pl.col('year') 
            + pl.lit(' (') + pl.col('month') + pl.lit(') ') + pl.col('month_name')
        )
        .when(pl.col('Time Period Type') == "Monthly").then(
            pl.lit('Month: ') + pl.col('year') 
            + pl.lit(' (') + pl.col('month') + pl.lit(') ') + pl.col('month_name')
        )
        .otherwise(None)
        .alias('period_lbl')
    )
    if df.filter(pl.col('period_lbl').is_null()).shape[0] > 0:
        print('time period is not consistent')
        print(
            df.filter(pl.col('period_lbl').is_null()).select(pl.col('Time Period Type').unique())
        )
    
    # %% read product order
    feature_groups = pl.read_excel('codes/feature_groups.xlsx').to_dict()
    product_order = dict(zip(feature_groups['Vendor Product ID'], feature_groups['product_code']))
    product_labels = dict(zip(feature_groups['Vendor Product ID'], feature_groups['full_label']))
    
    # %% collect product features
    
    product_feats = dict(zip(
        df['Vendor Product ID'].to_numpy(),
        # df['Product Name'].to_numpy(),
        df['Vendor Product ID'].replace_strict(product_labels).to_numpy(),
    ))
    
    graph = dict(zip(
        df['Vendor Product ID'].to_numpy(),
        df['Parent Product ID'].to_numpy()
    ))
    
    category = [i[0] for i in graph.items() if i[0] == i[1]]
    children = [i[0] for i in graph.items() if i[0] not in graph.values()]
    
    paths = {}
    
    for i in children:
        paths[i] = [i]
        lvl = i
        while lvl in graph:
            if lvl in category:
                break
            else:
                lvl = graph[lvl]
                paths[i].append(lvl)
    
    max_len = max([len(i) for i in paths.values()])
    
    for i in children:
        paths[i] = paths[i][::-1]
        for j in range(len(paths[i]), max_len + 1):
            paths[i].append(paths[i][-1])
    
    paths = {str(k): v for k, v in paths.items()}
    
    
    # %% product tree
    
    df_tree = pl.DataFrame(paths).transpose()
    df_tree.columns = [i.replace('column_', 'level_') for i in df_tree.columns]
    product_lvls = df_tree.columns
    df_tree = df_tree.sort(product_lvls)
    
    df_tree_hier = df_tree.select(product_lvls)
    df_tree_hier = df_tree_hier.with_columns(
        pl.lit('').alias(product_lvls[0]+'_rank')
    )
    for i in range(1, len(product_lvls)):
        df_tree_hier = df_tree_hier.with_columns(
            pl.col(product_lvls[i]).replace_strict(product_order)
            .rank('dense').over(partition_by=pl.col(product_lvls[i-1]))
            .alias(product_lvls[i]+'_rank')
        )
    
    df_tree_hier = df_tree_hier.select([i + '_rank' for i in product_lvls])
    df_tree_hier.columns = product_lvls
    
    for i in range(1, len(product_lvls)):
        df_tree_hier = df_tree_hier.with_columns(
            pl.when(
                df_tree[product_lvls[i-1]] != df_tree[product_lvls[i]]
            ).then(
            pl.col(product_lvls[i-1]).cast(str) 
            +
            pl.col(product_lvls[i]).cast(str)
            + pl.lit('.')
            ).otherwise(
                pl.col(product_lvls[i-1]).cast(str)
            ).alias(product_lvls[i])
        )
    
    product_feats_hier = dict(zip(
        df_tree.unpivot()['value'].to_list(),
        df_tree_hier.unpivot()['value'].to_list()
    ))
    
    product_feats_lvls = {k: str(len(v.split('.'))-1) for k, v in product_feats_hier.items()}
    
    product_feats_hier = {
        k: (product_feats_hier[k] + '0. ' + v).strip()
        for k, v in product_feats.items()
        }
    
    # %% add product features to tree
    dfs = []
    
    for i in range(1, len(df_tree.columns) - 1):
        dfs.append(
            df_tree.unique(df_tree.columns[:i*-1])
            .select(df_tree.columns[:i*-1])
            .with_columns([pl.col(df_tree.columns[i*-1-1]).alias(k) for k in df_tree.columns[i*-1:]])
        )
    
    dfs.append(pl.DataFrame(dict.fromkeys(df_tree.columns, category)))
    df_tree = pl.concat(dfs)
    df_tree = df_tree.with_columns(pl.col(df_tree.columns[-1]).alias('Vendor Product ID'))
    df_tree = df_tree.unique()
    
    
    # %% columns to keep
    
    metrics = [
        'Buying Households',
        'Projected Shoppers',
        'Panel Sample Size',
        'Raw Shoppers',
        'Loyalty Volume Based',
        'Loyalty Value Based Local Currency',
        'Loyalty Value Based USD',
        'Percent 2+ Time Buyers',
        'Percent Household Penetration',
        'Purchase Frequency',
        'Occasions',
        'Item Buying Rate local currency',
        'Item Buying Rate USD',
        'Purchase size local currency',
        'Purchase size USD',
        'Purchase size SU',
        'Average per SU local currency',
        'Average per SU USD',
        'Volume SU',
        'Volume Physical Units',
        'Spend Local Currency',
        'Spend USD']
    
    periods = ['Time Period Type', 'year', 'period_lbl']
    
    # %% join products
    
    df_join = (
        df.select(
            ['Category Name', 'Vendor Product ID', 'Product Name', 'Segment', 'Buyer Group ID', 'Buyer Group Name'] 
            + periods + metrics
        ).unique()
    ).join(
        df_tree, on='Vendor Product ID', how='left'
    )
    
    df_join = (
        df_join
        .with_columns([pl.col(i).alias(i + '_code') for i in product_lvls])
        .with_columns(
            pl.col(product_lvls).replace_strict(product_feats_hier)
        )
    )
    
    df_join = df_join.with_columns(
        pl.col('Vendor Product ID').replace_strict(product_feats_lvls).alias('product_lvls')
    )
    
    
    # save tree
    df_join.select(
        ['Category Name', 'Vendor Product ID', 'Product Name', 'product_lvls'] + product_lvls
    ).unique().write_excel(f'cat_trees/{name}_tree.xlsx', dtype_formats={pl.Int64: '0'})
    
    # %% read feature types
    feature_groups = pl.read_excel('codes/feature_groups.xlsx')\
        .filter(pl.col('Vendor Product ID').is_in(product_feats.keys())).to_dict()
    feature_groups = dict(zip(feature_groups['Vendor Product ID'], feature_groups['feature']))
    
    df_join = df_join.with_columns(
        pl.col('Vendor Product ID').replace_strict(feature_groups).alias('feature')
    )
    
    print(df_join.shape)
    
    # %% collect demo features 
    
    demo_feats = dict(zip(
        df['Buyer Group ID'].to_numpy(),
        df['Buyer Group Name'].to_numpy()
    ))
    
    graph = dict(zip(
        df['Buyer Group ID'].to_numpy(),
        df['Buyer Group Parent ID'].to_numpy()
    ))
    
    category = [i[0] for i in graph.items() if i[0] == i[1]]
    children = [i[0] for i in graph.items() if i[0] not in graph.values()]
    
    paths = {}
    
    for i in children:
        paths[i] = [i]
        lvl = i
        while lvl in graph:
            if lvl in category:
                break
            else:
                lvl = graph[lvl]
                paths[i].append(lvl)
       
    max_len = max([len(i) for i in paths.values()])
    
    for i in children:
        paths[i] = paths[i][::-1]
        for j in range(len(paths[i]), max_len + 1):
            paths[i].append(paths[i][-1])
    
    paths = {str(k): v for k, v in paths.items()}
    
    
    # %% read demo order
    demo_order = pl.read_excel('codes/demo_order.xlsx').to_dict()
    demo_order = dict(zip(demo_order['Buyer Group ID'], demo_order['demo_code']))
    
    # df['Buyer Group ID', 'Buyer Group Name', 'Buyer Group Parent ID'].unique().write_excel('check.xlsx')
    
    # %% collect demo features
    
    df_tree = pl.DataFrame(paths).transpose()
    df_tree.columns = [i.replace('column_', 'demo_') for i in df_tree.columns]
    demo_lvls = df_tree.columns
    df_tree = df_tree.sort(demo_lvls)
    
    df_tree_hier = df_tree.select(demo_lvls)
    df_tree_hier = df_tree_hier.with_columns(
        pl.lit('').alias(demo_lvls[0]+'_rank')
    )
    for i in range(1, len(demo_lvls)):
        df_tree_hier = df_tree_hier.with_columns(
            pl.col(demo_lvls[i]).replace_strict(demo_order).rank('dense').over(
                partition_by=pl.col(demo_lvls[i-1])).alias(demo_lvls[i]+'_rank')
    )
      
    df_tree_hier = df_tree_hier.select([i + '_rank' for i in demo_lvls])
    df_tree_hier.columns = demo_lvls
    
    for i in range(1, len(demo_lvls)):
        df_tree_hier = df_tree_hier.with_columns(
            pl.when(
                df_tree[demo_lvls[i-1]] != df_tree[demo_lvls[i]]
            ).then(
            pl.col(demo_lvls[i-1]).cast(str) 
            +
            pl.col(demo_lvls[i]).cast(str)
            + pl.lit('.')
            ).otherwise(
                pl.col(demo_lvls[i-1]).cast(str)
            ).alias(demo_lvls[i])
        )  
    
    
    demo_feats_hier = dict(zip(
        df_tree.unpivot()['value'].to_list(),
        df_tree_hier.unpivot()['value'].to_list()
    ))
    
    demo_feats_lvls = {k: str(len(v.split('.'))-1) for k, v in demo_feats_hier.items()}
    
    demo_feats_hier = {
        k: (demo_feats_hier[k] + '0. ' + v).strip()
        for k, v in demo_feats.items()
        }
    
    # %% add demo features to tree
    dfs = []
    
    for i in range(1, len(df_tree.columns) - 1):
        dfs.append(
            df_tree.unique(df_tree.columns[:i*-1])
            .select(df_tree.columns[:i*-1])
            .with_columns([pl.col(df_tree.columns[i*-1-1]).alias(k) for k in df_tree.columns[i*-1:]])
        )
        
    dfs.append(pl.DataFrame(dict.fromkeys(df_tree.columns, category)))
    df_tree = pl.concat(dfs, how='vertical_relaxed')
    df_tree = df_tree.with_columns(pl.col(df_tree.columns[-1]).alias('Buyer Group ID'))
    df_tree = df_tree.unique()
    
    # %% join demo
    
    df_join2 = (
        df_join.join(
            df_tree.with_columns(pl.col('Buyer Group ID').cast(pl.Int64)), 
            on='Buyer Group ID', how='left'
            )
    )
    
    df_join2 = (
        df_join2
        .with_columns([pl.col(i).alias(i + '_code') for i in demo_lvls])
        .with_columns(
            pl.col(demo_lvls).replace_strict(demo_feats_hier)
        )
    )
    
    df_join2 = df_join2.with_columns(
        pl.col('Buyer Group ID').replace_strict(demo_feats_lvls).alias('demo_lvls')
    )
    
    df_join2 = df_join2.select(
        ['Vendor Product ID', 'Product Name', 'feature', 'Segment', 'product_lvls']
        + product_lvls
        + [i + '_code' for i in product_lvls]
        + ['Buyer Group ID', 'Buyer Group Name', 'demo_lvls']
        + demo_lvls
        + [i + '_code' for i in demo_lvls]
        + periods
        + metrics
    )
    
    # drop demo lvl 1
    df_join2 = df_join2.filter(pl.col('demo_lvls') != "1")
    
    print(df.shape)
    print(df_join.shape)
    print(df_join2.shape)
    
    # %% drop redundant levels
    for i in range(len(product_lvls) - 1):
        if product_lvls[i*-1-1] in df_join2.columns:
            check = (
                df_join2
                .select((pl.col(product_lvls[i*-1-1])==pl.col(product_lvls[i*-1-2])).sum() == df_join2.shape[0])
            ).to_numpy()[0][0]
            
            if check:
                df_join2 = df_join2.select(
                    pl.exclude(
                        [product_lvls[i*-1-1], product_lvls[i*-1-1] + '_code']
                    )
                )
    
    
    for i in range(len(demo_lvls) - 1):
        if demo_lvls[i*-1-1] in df_join2.columns:
            check = (
                df_join2
                .select((pl.col(demo_lvls[i*-1-1])==pl.col(demo_lvls[i*-1-2])).sum() == df_join2.shape[0])
            ).to_numpy()[0][0]
            if check:
                df_join2 = df_join2.select(
                    pl.exclude(
                        [demo_lvls[i*-1-1], demo_lvls[i*-1-1] + '_code']
                    )
                )
             
    # %% remove levels (optimize)
    df_join2 = df_join2.rename(
        {'level_0_code': 'category_code', 
         'level_0': 'category',
         'demo_1_code': 'socdem_gr_code',
         'demo_1': 'socdem_gr'
         }
    ).select(pl.exclude(
        demo_lvls + [i + '_code' for i in demo_lvls]
        + product_lvls + [i + '_code' for i in product_lvls]
        )
    )
        
    df_join2 = df_join2.with_columns(
        pl.col('Vendor Product ID').replace_strict(product_feats_hier).alias('product_hier'),
        pl.col('Buyer Group ID').replace_strict(demo_feats_hier).alias('demo_hier'),
    )
    
    # %% write data
    # df_join2.write_csv(f'exports/{name}.csv', separator=',')
    
    # with zipfile.ZipFile(f'exports/{name}.zip', 'w') as zipf:
    #     zipf.write(
    #         f'exports/{name}.csv', 
    #         arcname = f'{name}.csv',
    #         compress_type = zipfile.ZIP_DEFLATED)
    # Path(f'exports/{name}.csv').unlink()
    
    df_join2.write_parquet(
        f'exports/{name}.pq.zstd', compression='zstd', compression_level=12)
    


