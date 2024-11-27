import pandas as pd
from tqdm import tqdm

files = [
    'PG_Flatfiles_Diapers',
    'PG_Flatfiles_BRMALE',
    'PG_Flatfiles_Shampoo',
    'PG_Flatfiles_Feminine_Care',
    'PG_Flatfiles_Hair_Conditioners',
    'PG_Flatfiles_Laundry_Detergents'
]

for f in tqdm(files):
    tmp = pd.read_csv(f'./data/old_format_data/{f}.zip', engine='pyarrow', compression='zip')
    # tmp.to_parquet(f'./data/{f}.pq.zstd', compression='zstd', engine='fastparquet')
    tmp.to_parquet(f'./data/old_format_data/{f}.pq.zstd', compression='zstd', engine='pyarrow')
