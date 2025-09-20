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

            # 新製品チーム（検査員マスタ H列=『新製品チーム』で★）から最低1名が割り当てられているかを可視化
            new_team_members = scheduler.get_new_product_team_members()
            if new_team_members:
                def _new_team_min1_flag(row):
                    if str(row.get('新製品', '')) == '★':
                        assigned_members = [m.strip() for m in str(row.get('割当メンバー', '')).split(',') if m.strip()]
                        return 'OK' if any(m in new_team_members for m in assigned_members) else 'NG'
                    return ''
                assignment_df['新チーム最低割当'] = assignment_df.apply(_new_team_min1_flag, axis=1)
            else:
                assignment_df['新チーム最低割当'] = assignment_df['新製品'].apply(lambda v: '' if str(v) != '★' else 'NG')

            # 表示用に納期はMM/DD
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
                    
                    # 新製品チーム（H列★）最低1名割当の達成状況
                    ok_min1 = len(new_product_df[new_product_df['新チーム最低割当'] == 'OK'])
                    ng_min1 = len(new_product_df[new_product_df['新チーム最低割当'] == 'NG'])
                    print(f"  新製品チーム(★)最低1名割当: OK {ok_min1}件 / NG {ng_min1}件")
                    if ng_min1 > 0:
                        print("  NG品目一覧（品番/納期/割当メンバー）:")
                        tmp = new_product_df[new_product_df['新チーム最低割当'] == 'NG'][['品番','納期','割当メンバー']].copy()
                        if '納期' in tmp.columns:
                            tmp['納期'] = pd.to_datetime(tmp['納期'], errors='coerce').dt.strftime('%m/%d')
                        print(tmp.to_string(index=False))

            # CSV保存（可視化列も含めて保存）
            formatter.save_to_csv(assignment_df, '検査員割当結果.csv', timestamp=True, decimals=2)
            # Excel保存（タスク別・作業員別シート含む）
            excel_path = formatter.save_assignment_report_excel(assignment_df, '検査員割当レポート.xlsx', timestamp=True, decimals=2)
            if excel_path:
                print(f"Excelレポートを出力しました: {excel_path}")
        else:
            print("\n検査員割当結果: データがありません。")

        # スキルベース検査員割当を実行
        print("\n" + "=" * 80)
        print("スキルベース検査員割当を実行します...")
        skill_assignment_df = scheduler.assign_inspectors_with_skill()
        if not skill_assignment_df.empty:
            print("\nスキルベース検査員割当結果")
            print("-" * 60)
            # 納期は表示用にMM/DDで
            display_skill_df = skill_assignment_df.copy()
            if '納期' in display_skill_df.columns:
                display_skill_df['納期'] = pd.to_datetime(display_skill_df['納期'], errors='coerce').dt.strftime('%m/%d')
            print(display_skill_df.to_string(index=False))
            
            # スキルベース割当の統計情報を表示
            total_products = len(skill_assignment_df)
            skill_matched_products = len(skill_assignment_df[skill_assignment_df['スキル情報'] != 'スキル情報なし'])
            fully_assigned = len(skill_assignment_df[skill_assignment_df['不足人員'] == 0])
            
            print(f"\nスキルベース割当統計:")
            print(f"  対象製品数: {total_products}件")
            print(f"  スキル対応可能: {skill_matched_products}件 / 全体: {total_products}件")
            print(f"  完全割当済: {fully_assigned}件 / 全体: {total_products}件")
            
            # スキルレベル別の割当状況
            skill_level_stats = {}
            for _, row in skill_assignment_df.iterrows():
                assigned_members = str(row.get('割当メンバー', ''))
                if assigned_members:
                    for member in assigned_members.split(','):
                        member = member.strip()
                        if 'スキル1' in member:
                            skill_level_stats['高スキル(1)'] = skill_level_stats.get('高スキル(1)', 0) + 1
                        elif 'スキル2' in member:
                            skill_level_stats['中スキル(2)'] = skill_level_stats.get('中スキル(2)', 0) + 1
                        elif 'スキル3' in member:
                            skill_level_stats['低スキル(3)'] = skill_level_stats.get('低スキル(3)', 0) + 1
                        elif '一般' in member:
                            skill_level_stats['一般割当'] = skill_level_stats.get('一般割当', 0) + 1
            
            if skill_level_stats:
                print(f"  スキルレベル別割当:")
                for skill_type, count in skill_level_stats.items():
                    print(f"    {skill_type}: {count}件")

            # CSV保存
            formatter.save_to_csv(skill_assignment_df, 'スキルベース検査員割当結果.csv', timestamp=True, decimals=2)
            # Excel保存（タスク別・作業員別シート含む）
            skill_excel_path = formatter.save_assignment_report_excel(skill_assignment_df, 'スキルベース検査員割当レポート.xlsx', timestamp=True, decimals=2)
            if skill_excel_path:
                print(f"Excelレポート（スキルベース）を出力しました: {skill_excel_path}")
        else:
            print("\nスキルベース検査員割当結果: データがありません。")
        
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

        # 新製品チーム（検査員マスタ H列=『新製品チーム』で★）から最低1名が割り当てられているかを可視化
        new_team_members = scheduler.get_new_product_team_members()
        if new_team_members:
            def _new_team_min1_flag(row):
                if str(row.get('新製品', '')) == '★':
                    assigned_members = [m.strip() for m in str(row.get('割当メンバー', '')).split(',') if m.strip()]
                    return 'OK' if any(m in new_team_members for m in assigned_members) else 'NG'
                return ''
            assignment_df['新チーム最低割当'] = assignment_df.apply(_new_team_min1_flag, axis=1)
        else:
            assignment_df['新チーム最低割当'] = assignment_df['新製品'].apply(lambda v: '' if str(v) != '★' else 'NG')

        # 別の処理ブロック: 表示用に納期はMM/DD
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
                
                # 新製品チーム（H列★）最低1名割当の達成状況
                ok_min1 = len(new_product_df[new_product_df['新チーム最低割当'] == 'OK'])
                ng_min1 = len(new_product_df[new_product_df['新チーム最低割当'] == 'NG'])
                print(f"  新製品チーム(★)最低1名割当: OK {ok_min1}件 / NG {ng_min1}件")
                if ng_min1 > 0:
                    print("  NG品目一覧（品番/納期/割当メンバー）:")
                    tmp = new_product_df[new_product_df['新チーム最低割当'] == 'NG'][['品番','納期','割当メンバー']].copy()
                    if '納期' in tmp.columns:
                        tmp['納期'] = pd.to_datetime(tmp['納期'], errors='coerce').dt.strftime('%m/%d')
                    print(tmp.to_string(index=False))

        # CSV保存（可視化列も含めて保存）
        formatter.save_to_csv(assignment_df, '検査員割当結果.csv', timestamp=True, decimals=2)
        # Excel保存（タスク別・作業員別シート含む）
        excel_path = formatter.save_assignment_report_excel(assignment_df, '検査員割当レポート.xlsx', timestamp=True, decimals=2)
        if excel_path:
            print(f"Excelレポートを出力しました: {excel_path}")
    else:
        print("\n検査員割当結果: データがありません。")

    # スキルベース検査員割当を実行
    print("\n" + "=" * 80)
    print("スキルベース検査員割当を実行します...")
    skill_assignment_df = scheduler.assign_inspectors_with_skill()
    if not skill_assignment_df.empty:
        print("\nスキルベース検査員割当結果")
        print("-" * 60)
        # 納期は表示用にMM/DDで
        display_skill_df = skill_assignment_df.copy()
        if '納期' in display_skill_df.columns:
            display_skill_df['納期'] = pd.to_datetime(display_skill_df['納期'], errors='coerce').dt.strftime('%m/%d')
        print(display_skill_df.to_string(index=False))
        
        # スキルベース割当の統計情報を表示
        total_products = len(skill_assignment_df)
        skill_matched_products = len(skill_assignment_df[skill_assignment_df['スキル情報'] != 'スキル情報なし'])
        fully_assigned = len(skill_assignment_df[skill_assignment_df['不足人員'] == 0])
        
        print(f"\nスキルベース割当統計:")
        print(f"  対象製品数: {total_products}件")
        print(f"  スキル対応可能: {skill_matched_products}件 / 全体: {total_products}件")
        print(f"  完全割当済: {fully_assigned}件 / 全体: {total_products}件")
        
        # スキルレベル別の割当状況
        skill_level_stats = {}
        for _, row in skill_assignment_df.iterrows():
            assigned_members = str(row.get('割当メンバー', ''))
            if assigned_members:
                for member in assigned_members.split(','):
                    member = member.strip()
                    if 'スキル1' in member:
                        skill_level_stats['高スキル(1)'] = skill_level_stats.get('高スキル(1)', 0) + 1
                    elif 'スキル2' in member:
                        skill_level_stats['中スキル(2)'] = skill_level_stats.get('中スキル(2)', 0) + 1
                    elif 'スキル3' in member:
                        skill_level_stats['低スキル(3)'] = skill_level_stats.get('低スキル(3)', 0) + 1
                    elif '一般' in member:
                        skill_level_stats['一般割当'] = skill_level_stats.get('一般割当', 0) + 1
        
        if skill_level_stats:
            print(f"  スキルレベル別割当:")
            for skill_type, count in skill_level_stats.items():
                print(f"    {skill_type}: {count}件")

        # CSV保存
        formatter.save_to_csv(skill_assignment_df, 'スキルベース検査員割当結果.csv', timestamp=True, decimals=2)
        # Excel保存（タスク別・作業員別シート含む）
        skill_excel_path = formatter.save_assignment_report_excel(skill_assignment_df, 'スキルベース検査員割当レポート.xlsx', timestamp=True, decimals=2)
        if skill_excel_path:
            print(f"Excelレポート（スキルベース）を出力しました: {skill_excel_path}")
    else:
        print("\nスキルベース検査員割当結果: データがありません。")


if __name__ == "__main__":
    # コマンドライン引数での日付指定に対応
    if len(sys.argv) > 1:
        run_analysis_with_date(sys.argv[1])
    else:
        main()
