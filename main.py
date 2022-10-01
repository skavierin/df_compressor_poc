import pandas as pd
import numpy as np
import pickle
import gzip
import sys


def get_dataset(size):
    df = pd.DataFrame()
    df['position'] = np.random.choice(['left', 'right', 'up', 'down'], size)
    df['age'] = np.random.randint(0, 100, size)
    df['team'] = np.random.choice(['red', 'blue'], size)
    return df


def compress_df(df):
    c_df = gzip.compress(pickle.dumps(df), compresslevel=1)
    del(df)
    return c_df


def decompress_df(c_df):
    df = pickle.loads(gzip.decompress(c_df))
    del(c_df)
    return df


raw_df = get_dataset(2000000)

raw_size = sys.getsizeof(raw_df)
print(raw_size)

compressed_df = compress_df(raw_df)
compressed_size = sys.getsizeof(compressed_df)
print(compressed_size)

raw_df_2 = decompress_df(compressed_df)
raw_size_2 = sys.getsizeof(raw_df_2)
print(raw_size_2)

print(pickle.dumps(raw_df) == pickle.dumps(raw_df_2))
print(f'{round((float(compressed_size)/raw_size)*100, 2)}%')
