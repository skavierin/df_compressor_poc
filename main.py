import gzip
import pickle
import random
import string
import sys
import os
import time

import numpy as np
import pandas as pd


TEMP_DIR = '/tmp/Custom'
os.makedirs(TEMP_DIR, exist_ok=True)

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


def dump_df(df):
    dump_name = ''.join([random.choice(string.ascii_letters) for _ in range(12)]) + '.df'
    with open(os.path.join(TEMP_DIR, dump_name), mode = 'wb') as f:
        pickle.dump(df, f)
    return dump_name

def load_df(dump_name):
    df = None
    with open(os.path.join(TEMP_DIR, dump_name), mode = 'rb') as f:
        df = pickle.load(f)
    os.remove(os.path.join(TEMP_DIR, dump_name))
    return df



raw_df = get_dataset(5000000)
df = raw_df.copy()

print('Running in memory compression')

time_1 = time.time()

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

time_2 = time.time()
print(f'Task took {time_2-time_1} seconds')


print('Running dump')

raw_size = sys.getsizeof(df)
print(raw_size)

compressed_df = dump_df(df)
compressed_size = sys.getsizeof(compressed_df)
print(compressed_size)

raw_df_2 = load_df(compressed_df)
raw_size_2 = sys.getsizeof(raw_df_2)
print(raw_size_2)

print(pickle.dumps(df) == pickle.dumps(raw_df_2))
print(f'{round((float(compressed_size)/raw_size)*100, 2)}%')

time_3 = time.time()
print(f'Task took {time_3-time_2} seconds')
