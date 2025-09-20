"""
結果出力モジュール
検査スケジュール分析結果をコンソールやファイルに出力する
"""

import pandas as pd
from datetime import datetime
from typing import Dict, Optional, Any
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

class OutputFormatter:
    """結果出力フォーマッタークラス"""

    def __init__(self, output_dir: str = "output"):
        """
        初期化
        Args:
            output_dir: 出力ディレクトリのパス
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)

    def print_urgent_products(self, urgent_products: pd.DataFrame, title: str = "緊急対応が必要な製品") -> None:
        """
        緊急対応製品をコンソールに出力
        Args:
            urgent_products: 緊急対応製品データフレーム
            title: タイトル
        """
        print(f"\n{'='*80}")
        print(f"{title}")
        print(f"{'='*80}")

        if urgent_products.empty:
            print("該当する製品はありません。")
            return

        print(f"該当製品数: {len(urgent_products)}件\n")

        # 重要な列のみを表示（緊急度関連は除外）
        display_columns = [
            '�i��', '�H���ԍ�', '工程番号一覧', '�[��', '�s����', '�K�v���b�g��',
            '��������', '����������', '�����J�n����', '�����܂ł̓���'
        ]

        available_columns = [col for col in display_columns if col in urgent_products.columns]
        display_df = urgent_products[available_columns].head(20)  # 上位20件のみ表示

        # 日付フォーマットを調整
        if '納期' in display_df.columns:
            display_df = display_df.copy()
            display_df['納期'] = display_df['納期'].dt.strftime('%m/%d')

        if '検査開始期限' in display_df.columns:
            display_df['検査開始期限'] = display_df['検査開始期限'].dt.strftime('%m/%d %H:%M')

        # テーブル形式で出力
        print(display_df.to_string(index=False, max_colwidth=15))

        if len(urgent_products) > 20:
            print(f"\n... 他 {len(urgent_products) - 20} 件")

    def print_summary(self, summary: Dict[str, Any], title: str = "スケジュール概要") -> None:
        """
        概要をコンソールに出力
        Args:
            summary: 概要データ
            title: タイトル
        """
        print(f"\n{'='*60}")
        print(f"{title}")
        print(f"{'='*60}")

        if not summary or "error" in summary:
            print("概要データが取得できませんでした。")
            return

        # 緊急度別集計は非表示

        # 時間統計
        if '総検査時間' in summary:
            print(f"\n■ 時間統計:")
            print(f"  総検査時間      : {summary['総検査時間']:>8.1f}時間")
            if summary.get('平均検査時間'):
                print(f"  平均検査時間    : {summary['平均検査時間']:>8.2f}時間")

        # 期限に関する統計
        period_stats = [
            ('期限超過', summary.get('期限超過製品数', 0)),
            ('今日開始必要', summary.get('今日開始必要', 0)),
            ('1日以内開始', summary.get('1日以内', 0)),
            ('3日以内開始', summary.get('3日以内', 0))
        ]

        print(f"\n■ 期限統計:")
        for label, count in period_stats:
            if count > 0:
                print(f"  {label:<12}: {count:>6}件")

    def print_capacity_analysis(self, capacity: Dict[str, Any], title: str = "検査員リソース分析") -> None:
        """
        キャパシティ分析をコンソールに出力
        Args:
            capacity: キャパシティ分析データ
            title: タイトル
        """
        print(f"\n{'='*60}")
        print(f"{title}")
        print(f"{'='*60}")

        if not capacity or "error" in capacity:
            print("検査員データが取得できませんでした。")
            return

        print(f"総検査員数: {capacity.get('総検査員数', 0)}名")

        # グループ別人数
        if 'グループ別人数' in capacity and capacity['グループ別人数']:
            print("\n■ グループ別人数:")
            for group, count in capacity['グループ別人数'].items():
                print(f"  グループ{group}: {count}名")

        # 勤務時間分析
        if '勤務時間分析' in capacity and capacity['勤務時間分析']:
            working_hours = capacity['勤務時間分析']
            if working_hours:
                print("\n■ 勤務時間分析:")
                if '平均勤務時間' in working_hours:
                    print(f"  平均勤務時間: {working_hours['平均勤務時間']:.1f}時間")
                if '最大勤務時間' in working_hours:
                    print(f"  最大勤務時間: {working_hours['最大勤務時間']:.1f}時間")

    def save_to_csv(self, data: pd.DataFrame, filename: str, timestamp: bool = True, decimals: int = 2) -> str:
        """
        データをCSVファイルに保存

        Args:
            data: 保存するデータフレーム
            filename: ファイル名
            timestamp: タイムスタンプを付加するか
            decimals: 総検査時間の丸め桁数

        Returns:
            str: 保存されたファイルパス
        """
        if timestamp:
            timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
            name, ext = filename.rsplit('.', 1)
            filename = f"{name}_{timestamp_str}.{ext}"

        file_path = self.output_dir / filename

        try:
            export_df = data.copy()

            # 総検査時間を指定された桁数で丸める
            if '総検査時間' in export_df.columns and decimals is not None:
                export_df['総検査時間'] = pd.to_numeric(export_df['総検査時間'], errors='coerce').round(decimals)

            # ファイル名にタイムスタンプ
            export_df.to_csv(file_path, index=False, encoding='utf-8-sig')
            logger.info(f"CSVファイルを保存しました: {file_path}")
            return str(file_path)
        except Exception as e:
            logger.error(f"CSV保存エラー: {e}")
            return ""

    # 追加: 作業員別の割当時間シートを含むExcelレポート保存
    def save_assignment_report_excel(self, assignment_df: pd.DataFrame, filename: str = '検査員割当レポート.xlsx', timestamp: bool = True, decimals: int = 2) -> str:
        """
        割当結果からExcelレポートを作成し、
        - タスク別割当（元データ）
        - 作業員別割当時間（集計）
        のシートを出力する

        Args:
            assignment_df: 検査員割当結果のDataFrame（列: 品番, 工程番号, 納期, 総検査時間, 割当人数, 割当メンバー, など）
            filename: 出力するExcelファイル名
            timestamp: タイムスタンプ付与の有無
            decimals: 時間の丸め桁数

        Returns:
            str: 保存されたExcelファイルのパス
        """
        try:
            if assignment_df is None or assignment_df.empty:
                logger.warning("assignment_df が空のため、Excelレポートを作成しませんでした")
                return ""

            # タイムスタンプ
            if timestamp:
                timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
                name, ext = filename.rsplit('.', 1)
                filename = f"{name}_{timestamp_str}.{ext}"

            file_path = self.output_dir / filename

            # 作業員別の明細行を作成
            rows = []
            for _, row in assignment_df.iterrows():
                try:
                    task_time = pd.to_numeric(row.get('総検査時間', 0), errors='coerce')
                except Exception:
                    task_time = 0
                members_raw = str(row.get('割当メンバー', '') or '')
                members = [m.strip() for m in members_raw.split(',') if m.strip()]

                if task_time is None or pd.isna(task_time) or task_time <= 0:
                    continue
                if not members:
                    continue

                assigned_count = len(members)
                # 単純按分: 総検査時間を割当人数で等分
                per_member_time = task_time if assigned_count == 1 else task_time / max(assigned_count, 1)

                for mem in members:
                    rows.append({
                        '作業員': mem,
                        '品番': row.get('品番', ''),
                        '工程番号': row.get('工程番号', ''),
                        '納期': row.get('納期', ''),
                        '割当時間': per_member_time
                    })

            if not rows:
                logger.warning("割当メンバーの明細が空でした。Excelレポートの作業員別シートは作成されません")
                # タスク別シートのみ出力
                with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
                    assignment_df.to_excel(writer, index=False, sheet_name='タスク別割当')
                logger.info(f"Excelファイルを保存しました: {file_path}")
                return str(file_path)

            detail_df = pd.DataFrame(rows)
            # 丸め
            detail_df['割当時間'] = pd.to_numeric(detail_df['割当時間'], errors='coerce').round(decimals)

            # 作業員別の集計
            summary_df = detail_df.groupby('作業員', as_index=False).agg(
                割当タスク数=('作業員', 'count'),
                合計割当時間=('割当時間', 'sum')
            )
            summary_df['平均割当時間'] = (summary_df['合計割当時間'] / summary_df['割当タスク数']).round(decimals)
            summary_df['合計割当時間'] = summary_df['合計割当時間'].round(decimals)

            # Excelへ出力
            with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
                # 1. 元の割当結果
                assignment_df.to_excel(writer, index=False, sheet_name='タスク別割当')
                # 2. 作業員別集計
                summary_df.to_excel(writer, index=False, sheet_name='作業員別割当時間')
                # 3. 明細（参考）
                detail_df.to_excel(writer, index=False, sheet_name='作業員-タスク明細')

            logger.info(f"Excelレポートを保存しました: {file_path}")
            return str(file_path)
        except Exception as e:
            logger.error(f"Excelレポート保存エラー: {e}")
            return ""

    def generate_full_report(self, urgent_products: pd.DataFrame, summary: Dict, capacity: Dict) -> None:
        """
        完全レポートを生成・出力
        Args:
            urgent_products: 緊急対応製品
            summary: スケジュール概要
            capacity: キャパシティ分析
        """
        print(f"\n検査スケジュール分析レポート - {datetime.now().strftime('%Y年%m月%d日 %H:%M')}")
        print("=" * 80)

        # 各セクションを出力（緊急度関連の詳細出力は行わない）
        self.print_summary(summary)
        self.print_capacity_analysis(capacity)

        # 緊急製品のリスト表示とCSV保存は不要のため出力しない

    def create_action_plan(self, urgent_products: pd.DataFrame) -> None:
        """
        アクションプランを生成・出力
        Args:
            urgent_products: 緊急対応製品
        """
        print(f"\n{'='*80}")
        print("推奨アクションプラン")
        print(f"{'='*80}")

        if urgent_products.empty:
            print("緊急対応が必要な製品はありません。")
            return

        # 緊急度レベル列の存在確認
        if '緊急度レベル' not in urgent_products.columns:
            print("緊急度レベル情報が利用できません。")
            return

        # 最緊急製品（1日以内）
        most_urgent = urgent_products[urgent_products['緊急度レベル'] == 1]
        if not most_urgent.empty:
            print(f"\n【即時対応必要】{len(most_urgent)}件")
            print("今日中に検査を開始する必要があります:")
            for _, product in most_urgent.head(5).iterrows():
                lot_info = f"ロット数:{product.get('必要ロット数', 'N/A')}" if '必要ロット数' in product else f"不足数:{product.get('不足数', 'N/A')}"
                print(f"  - {product['品番']} (工程:{product.get('工程番号', 'N/A')}) ({lot_info}, 検査時間:{product.get('総検査時間', product.get('検査時間', 'N/A'))}h)")

        # 緊急製品（3日以内）
        urgent = urgent_products[urgent_products['緊急度レベル'] == 2]
        if not urgent.empty:
            print(f"\n【3日以内対応】{len(urgent)}件")
            print("3日以内に検査開始スケジュールを調整してください:")
            for _, product in urgent.head(5).iterrows():
                print(f"  - {product['品番']} (工程:{product.get('工程番号', 'N/A')}) (期限まで:{product['期限までの日数']}日)")

        # 総検査時間の確認
        if '総検査時間' in urgent_products.columns:
            total_hours = urgent_products['総検査時間'].sum()
            print(f"\n【リソース確認】")
            print(f"総検査時間: {total_hours:.1f}時間")
            print(f"必要検査員数（1日8時間換算）: {total_hours/8:.1f}名日")