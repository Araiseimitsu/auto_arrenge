"""
検査スケジュール計算システム - メインエントリポイント
出荷不足製品の検査スケジュールを計算し、緊急対応が必要な製品を特定します
"""

import sys
import logging
from datetime import datetime
import pandas as pd

from src.inspection_scheduler import InspectionScheduler
from src.output_formatter import OutputFormatter

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """メイン処理"""
    print("検査スケジュール計算システムを開始します...")
    print("=" * 80)

    try:
        # システム初期化
        # 製品マスタの検査時間単位を強制的に 'seconds' として扱う設定
        config = {
            'product_master_time_unit': 'seconds'
        }
        scheduler = InspectionScheduler(data_loader_config=config)
        formatter = OutputFormatter()

        # 完全分析を実行
        urgent_products, schedule_summary, capacity_analysis = scheduler.run_full_analysis()

        # 結果出力（緊急度関連の詳細出力は省略）
        formatter.generate_full_report(urgent_products, schedule_summary, capacity_analysis)

        # 検査員割当を実行（納期が早い順）
        assignment_df = scheduler.assign_inspectors()
        if not assignment_df.empty:
            print("\n検査員割当結果（納期が早い順）")
            print("-" * 60)
            # 納期は表示用にMM/DDで
            display_df = assignment_df.copy()
            if '納期' in display_df.columns:
                display_df['納期'] = pd.to_datetime(display_df['納期'], errors='coerce').dt.strftime('%m/%d')
            print(display_df.to_string(index=False))
            
            # 新製品対応の統計情報を表示
            if '新製品' in assignment_df.columns:
                new_product_count = len(assignment_df[assignment_df['新製品'] == '★'])
                total_count = len(assignment_df)
                print(f"\n新製品対応統計:")
                print(f"  新製品件数: {new_product_count}件 / 全体: {total_count}件")
                if new_product_count > 0:
                    new_product_df = assignment_df[assignment_df['新製品'] == '★']
                    assigned_new_products = len(new_product_df[new_product_df['割当人数'] > 0])
                    print(f"  新製品割当済: {assigned_new_products}件 / 新製品: {new_product_count}件")
                    
                    # 新製品チームメンバーが割り当てられた件数
                    new_team_members = scheduler.get_new_product_team_members()
                    if new_team_members:
                        new_team_assigned = 0
                        for _, row in new_product_df.iterrows():
                            assigned_members = str(row.get('割当メンバー', '')).split(',')
                            if any(member.strip() in new_team_members for member in assigned_members if member.strip()):
                                new_team_assigned += 1
                        print(f"  新製品チーム対応: {new_team_assigned}件 / 新製品: {new_product_count}件")

            # CSV保存
            formatter.save_to_csv(assignment_df, '検査員割当結果.csv', timestamp=True, decimals=2)
        else:
            print("\n検査員割当結果: データがありません。")
        print("\n" + "=" * 80)
        print("処理が完了しました。")

    except Exception as e:
        logger.error(f"システムエラーが発生しました: {e}")
        print(f"エラー: {e}")
        sys.exit(1)

def run_analysis_with_date(target_date: str = None):
    """指定日付での分析実行"""
    base_date = None
    if target_date:
        try:
            base_date = datetime.strptime(target_date, "%Y-%m-%d")
            print(f"基準日を {target_date} に設定しました")
        except ValueError:
            print(f"日付形式が正しくありません: {target_date} (YYYY-MM-DD形式で入力してください)")
            return

    scheduler = InspectionScheduler(base_date=base_date)
    formatter = OutputFormatter()

    urgent_products, schedule_summary, capacity_analysis = scheduler.run_full_analysis()
    formatter.generate_full_report(urgent_products, schedule_summary, capacity_analysis)

    # 検査員割当も実行
    assignment_df = scheduler.assign_inspectors()
    if not assignment_df.empty:
        # 関数・処理の一部
        print("\n検査員割当結果（納期が早い順）")
        print("-" * 60)
        # 別の処理ブロック
        display_df = assignment_df.copy()
        if '納期' in display_df.columns:
            display_df['納期'] = pd.to_datetime(display_df['納期'], errors='coerce').dt.strftime('%m/%d')
        print(display_df.to_string(index=False))
        
        # 新製品対応の統計情報を表示
        if '新製品' in assignment_df.columns:
            new_product_count = len(assignment_df[assignment_df['新製品'] == '★'])
            total_count = len(assignment_df)
            print(f"\n新製品対応統計:")
            print(f"  新製品件数: {new_product_count}件 / 全体: {total_count}件")
            if new_product_count > 0:
                new_product_df = assignment_df[assignment_df['新製品'] == '★']
                assigned_new_products = len(new_product_df[new_product_df['割当人数'] > 0])
                print(f"  新製品割当済: {assigned_new_products}件 / 新製品: {new_product_count}件")
                
                # 新製品チームメンバーが割り当てられた件数
                new_team_members = scheduler.get_new_product_team_members()
                if new_team_members:
                    new_team_assigned = 0
                    for _, row in new_product_df.iterrows():
                        assigned_members = str(row.get('割当メンバー', '')).split(',')
                        if any(member.strip() in new_team_members for member in assigned_members if member.strip()):
                            new_team_assigned += 1
                    print(f"  新製品チーム対応: {new_team_assigned}件 / 新製品: {new_product_count}件")

        # CSV保存
        formatter.save_to_csv(assignment_df, '検査員割当結果.csv', timestamp=True, decimals=2)
    else:
        print("\n検査員割当結果: データがありません。")


if __name__ == "__main__":
    # コマンドライン引数での日付指定に対応
    if len(sys.argv) > 1:
        run_analysis_with_date(sys.argv[1])
    else:
        main()
