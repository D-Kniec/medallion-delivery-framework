import pandas as pd

df = pd.read_csv('src/bronze/customers/customers.csv')
df['raw_id'] = df.index
df.to_csv('src/bronze/customers/customers.csv')
print(df)