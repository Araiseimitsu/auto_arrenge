import pandas as pd
import numpy as np
from pathlib import Path

def analyze_process_number_mismatch():
    """
    工程番号不一致の根本原因分析と解決策の提示
    """
    print("="*80)
    print("工程番号不一致問題の根本原因分析レポート")
    print("="*80)
    
    # データ読み込み
    try:
        shortage_df = pd.read_excel('src/data/出荷不足20250919.xlsx')
        master_df = pd.read_excel('src/data/製品マスタ.xlsx')
    except Exception as e:
        print(f"ファイル読み込みエラー: {e}")
        return
    
    print("\n1. 問題の概要")
    print("-" * 40)
    print("出力された工程番号と製品マスタの工程番号が一致しない問題が発生しています。")
    print("具体例:")
    print("  出力: 16H-001-04(転造・貫通孔有) 工程番号: 4")
    print("  マスタ: 16H-001-04(転造・貫通孔有) 工程番号: 3")
    
    print("\n2. 根本原因の特定")
    print("-" * 40)
    print("コード分析の結果、以下の処理フローが原因であることが判明:")
    print("")
    print("【現在の処理フロー】")
    print("1. 出荷不足データから「現在工程番号」(N列)を取得")
    print("2. DateCalculator.add_time_calculations()で以下の処理:")
    print("   - 出荷不足データの「現在工程番号」を「工程番号」として使用")
    print("   - 製品マスタの「工程番号」は検査時間取得のみに使用")
    print("3. 最終出力では出荷不足データの「現在工程番号」が表示される")
    print("")
    print("【問題点】")
    print("- 出荷不足データの「現在工程番号」= 製造進捗上の現在位置")
    print("- 製品マスタの「工程番号」= 設計上の標準工程定義")
    print("- この2つは本来異なる概念であり、直接比較すべきではない")
    
    # 実際のデータで確認
    print("\n3. 実データでの確認")
    print("-" * 40)
    
    # 出荷不足データの該当品番
    target_parts = shortage_df[shortage_df['品番'].str.startswith(('16H-001-04', '16H-001-03'), na=False)]
    if not target_parts.empty:
        print("【出荷不足データ - 現在工程番号】")
        for _, row in target_parts.iterrows():
            part_no = row.get('品番', 'N/A')
            current_process = row.get('現在工程番号', 'N/A')
            print(f"  {part_no}: {current_process}")
    
    # 製品マスタの該当品番
    master_target = master_df[master_df['品番'].str.startswith(('16H-001-04', '16H-001-03'), na=False)]
    if not master_target.empty:
        print("\n【製品マスタ - 工程番号】")
        for _, row in master_target.iterrows():
            part_no = row.get('品番', 'N/A')
            process_no = row.get('工程番号', 'N/A')
            print(f"  {part_no}: {process_no}")
    
    print("\n4. 解決策の提案")
    print("-" * 40)
    print("以下の3つの解決策を提案します:")
    print("")
    print("【解決策A: 表示方法の改善】")
    print("- 出力時に「現在工程: X, 標準工程: Y」のように両方表示")
    print("- ユーザーが両方の情報を把握できる")
    print("- 実装が比較的簡単")
    print("")
    print("【解決策B: データ統合の改善】")
    print("- 製品マスタに「現在工程番号」の概念を追加")
    print("- 出荷不足データと製品マスタの工程番号定義を統一")
    print("- 根本的な解決だが、データ構造の変更が必要")
    print("")
    print("【解決策C: 検査時間取得ロジックの改善】")
    print("- 現在工程番号に対応する検査時間を正確に取得")
    print("- 工程番号の不一致があっても適切な検査時間を使用")
    print("- 現在のシステムへの影響を最小化")
    
    print("\n5. 推奨実装")
    print("-" * 40)
    print("【短期対応】解決策A: 表示改善")
    print("- OutputFormatterで「現在工程: X (標準: Y)」形式で表示")
    print("- ユーザーに混乱を与えないよう明確に区別")
    print("")
    print("【中長期対応】解決策B: データ統合")
    print("- 製品マスタと出荷不足データの工程番号定義を統一")
    print("- 業務フローとシステムの整合性を確保")
    
    print("\n6. 具体的な修正箇所")
    print("-" * 40)
    print("【修正対象ファイル】")
    print("1. src/output_formatter.py")
    print("   - print_urgent_products()メソッドの表示形式変更")
    print("2. src/date_calculator.py")
    print("   - add_time_calculations()メソッドの工程番号処理改善")
    print("3. src/data_loader.py")
    print("   - 工程番号の標準化処理の見直し")
    
    print("\n" + "="*80)
    print("分析完了: 工程番号不一致の原因は出荷不足データの「現在工程番号」と")
    print("製品マスタの「工程番号」が異なる概念であることが根本原因です。")
    print("="*80)

if __name__ == "__main__":
    analyze_process_number_mismatch()