import pandas as pd

# 出荷不足データを読み込み
df = pd.read_excel('src/data/出荷不足20250919.xlsx', engine='openpyxl')

print('全列名:')
for i, col in enumerate(df.columns):
    print(f'{i}: {col}')

print('\n16H-001-04で始まる品番のデータ:')
filtered_04 = df[df['品番'].astype(str).str.contains('16H-001-04', na=False)]
print(filtered_04[['出荷予定日', '品番', '出荷数', '不足数', '生産ロットID', 'ロット数量', '現在工程番号']].head(10))

print('\n16H-001-03で始まる品番のデータ:')
filtered_03 = df[df['品番'].astype(str).str.contains('16H-001-03', na=False)]
print(filtered_03[['出荷予定日', '品番', '出荷数', '不足数', '生産ロットID', 'ロット数量', '現在工程番号']].head(10))

# 製品マスタも確認
print('\n\n製品マスタデータ:')
master_df = pd.read_excel('src/data/製品マスタ.xlsx', engine='openpyxl')
print('製品マスタの列名:')
for i, col in enumerate(master_df.columns):
    print(f'{i}: {col}')

print('\n16H-001-04で始まる製品マスタデータ:')
master_04 = master_df[master_df.iloc[:, 1].astype(str).str.contains('16H-001-04', na=False)]
print(master_04.iloc[:, [1, 3, 4]].head(10))  # 品番、工程番号、検査時間

print('\n16H-001-03で始まる製品マスタデータ:')
master_03 = master_df[master_df.iloc[:, 1].astype(str).str.contains('16H-001-03', na=False)]
print(master_03.iloc[:, [1, 3, 4]].head(10))  # 品番、工程番号、検査時間