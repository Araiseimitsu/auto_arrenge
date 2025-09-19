import pandas as pd

# データを読み込み
shortage_df = pd.read_excel('src/data/出荷不足20250919.xlsx', engine='openpyxl')
master_df = pd.read_excel('src/data/製品マスタ.xlsx', engine='openpyxl')

print("=== 工程番号不一致の原因分析 ===")
print()

# 出荷不足データの16H-001-04品番の工程番号
print("【出荷不足データ】16H-001-04品番の現在工程番号:")
shortage_04 = shortage_df[shortage_df['品番'].astype(str).str.contains('16H-001-04', na=False)]
for _, row in shortage_04.iterrows():
    print(f"  {row['品番']}: 現在工程番号 = {row['現在工程番号']}")

print()

# 製品マスタの16H-001-04品番の工程番号
print("【製品マスタ】16H-001-04品番の工程番号:")
master_04 = master_df[master_df.iloc[:, 1].astype(str).str.contains('16H-001-04', na=False)]
for _, row in master_04.iterrows():
    print(f"  {row.iloc[1]}: 工程番号 = {row.iloc[3]}")

print()
print("=== 不一致の詳細分析 ===")
print()

# 具体的な不一致を確認
print("出力結果と製品マスタの比較:")
print("出力結果:")
print("  16H-001-04(ﾀｯﾌﾟ・貫通孔有): 4")
print("  16H-001-04(ｽﾚｯﾄﾞﾐﾙ・貫通孔無): 2")
print("  16H-001-04(ｽﾚｯﾄﾞﾐﾙ・貫通孔有): 4")
print("  16H-001-04(転造・貫通孔有): 4")
print("  16H-001-04(ﾀｯﾌﾟ・貫通孔無): 4")
print()
print("製品マスタ:")
print("  16H-001-04(転造・貫通孔有): 3")
print("  16H-001-04(ﾀｯﾌﾟ・貫通孔有): 4")
print("  16H-001-04(ﾀｯﾌﾟ・貫通孔無): 3")
print()

# 原因の特定
print("=== 原因の特定 ===")
print()
print("1. 出力結果は【出荷不足データの現在工程番号】を使用している")
print("2. 製品マスタは【設計上の標準工程番号】を持っている")
print("3. 現在工程番号は製造進捗を表し、製品マスタの工程番号とは異なる概念")
print()
print("具体的な不一致:")
print("- 16H-001-04(転造・貫通孔有): 出力=4, マスタ=3")
print("- 16H-001-04(ﾀｯﾌﾟ・貫通孔無): 出力=4, マスタ=3")
print("- 16H-001-04(ｽﾚｯﾄﾞﾐﾙ・貫通孔無): 出力=2, マスタ=データなし")
print("- 16H-001-04(ｽﾚｯﾄﾞﾐﾙ・貫通孔有): 出力=4, マスタ=データなし")
print()
print("結論: 出力は実際の製造進捗（現在工程番号）を表示しており、")
print("      製品マスタの設計工程番号とは異なるデータソースを使用している")