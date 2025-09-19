#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
工程番号不一致問題のデバッグスクリプト
各段階での工程番号の変化を追跡します
"""

import pandas as pd
import sys
import os
from pathlib import Path

# srcディレクトリをパスに追加
sys.path.append(str(Path(__file__).parent / 'src'))

from src.data_loader import DataLoader
from src.inspection_scheduler import InspectionScheduler

def debug_process_numbers():
    """工程番号の変化を段階的に追跡"""
    print("=" * 80)
    print("工程番号不一致問題のデバッグ開始")
    print("=" * 80)
    
    try:
        # データローダーの初期化
        config = {'product_master_time_unit': 'seconds'}
        data_loader = DataLoader(data_dir='src/data', config=config)
        
        print("\n1. 出荷不足データの読み込み")
        print("-" * 40)
        shortage_data = data_loader.load_shortage_data()
        
        # 対象品番のフィルタリング
        target_products = ['KBS-4', '16H-001-04']
        shortage_filtered = shortage_data[shortage_data['品番'].str.contains('|'.join(target_products), na=False)]
        
        print("出荷不足データの工程番号:")
        for _, row in shortage_filtered.iterrows():
            print(f"  品番: {row['品番']}, 工程番号: {row.get('現在工程番号', 'N/A')}")
        
        print("\n2. 製品マスタデータの読み込み")
        print("-" * 40)
        product_master = data_loader.load_product_master()
        
        # 対象品番のフィルタリング
        master_filtered = product_master[product_master['品番'].str.contains('|'.join(target_products), na=False)]
        
        print("製品マスタの工程番号:")
        for _, row in master_filtered.iterrows():
            print(f"  品番: {row['品番']}, 工程番号: {row.get('工程番号', 'N/A')}")
        
        print("\n3. 全データ読み込み後の状態確認")
        print("-" * 40)
        all_data = data_loader.load_all_data()
        
        # all_dataの構造を確認
        print(f"all_dataの型: {type(all_data)}")
        if isinstance(all_data, dict):
            print(f"all_dataのキー: {list(all_data.keys())}")
            if 'shortage_data' in all_data:
                shortage_after = all_data['shortage_data']
                shortage_after_filtered = shortage_after[shortage_after['品番'].str.contains('|'.join(target_products), na=False)]
                
                print("全データ読み込み後の出荷不足データ:")
                for _, row in shortage_after_filtered.iterrows():
                    process_cols = [col for col in row.index if '工程' in col]
                    print(f"  品番: {row['品番']}")
                    for col in process_cols:
                        print(f"    {col}: {row[col]}")
        else:
            print("all_dataは辞書型ではありません。スキップします。")
        
        print("\n4. InspectionSchedulerでの処理確認")
        print("-" * 40)
        scheduler = InspectionScheduler(data_loader_config=config)
        
        # データ読み込み
        scheduler.load_data()
        
        # 検査スケジュール計算
        schedule_result = scheduler.calculate_schedules()
        
        if not schedule_result.empty:
            schedule_filtered = schedule_result[schedule_result['品番'].str.contains('|'.join(target_products), na=False)]
            
            print("検査スケジュール計算後:")
            for _, row in schedule_filtered.iterrows():
                process_cols = [col for col in row.index if '工程' in col]
                print(f"  品番: {row['品番']}")
                for col in process_cols:
                    print(f"    {col}: {row[col]}")
        
        print("\n5. 最終出力での工程番号確認")
        print("-" * 40)
        urgent_products, _, _ = scheduler.run_full_analysis()
        
        if not urgent_products.empty:
            urgent_filtered = urgent_products[urgent_products['品番'].str.contains('|'.join(target_products), na=False)]
            
            print("最終出力での工程番号:")
            for _, row in urgent_filtered.iterrows():
                process_cols = [col for col in row.index if '工程' in col]
                print(f"  品番: {row['品番']}")
                for col in process_cols:
                    print(f"    {col}: {row[col]}")
        
        print("\n" + "=" * 80)
        print("デバッグ完了")
        print("=" * 80)
        
    except Exception as e:
        print(f"エラーが発生しました: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_process_numbers()