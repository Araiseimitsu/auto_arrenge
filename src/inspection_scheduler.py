"""
検査スケジューラーメインロジック
製品の検査スケジュールを計算し、緊急対応が必要な製品を特定する
"""

import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import logging

from .data_loader import DataLoader
from .date_calculator import DateCalculator

logger = logging.getLogger(__name__)

class InspectionScheduler:
    """検査スケジューラークラス"""

    def __init__(self, data_dir: str = "src/data", base_date: Optional[datetime] = None, data_loader_config: Optional[Dict] = None):
        """
        初期化
        Args:
            data_dir: データディレクトリのパス
            base_date: 基準日（現在日時）
            data_loader_config: DataLoaderに渡す設定
        """
        self.data_loader = DataLoader(data_dir, config=data_loader_config)
        self.date_calculator = DateCalculator(base_date)

        # データキャッシュ
        self.shortage_data: Optional[pd.DataFrame] = None
        self.product_master: Optional[pd.DataFrame] = None
        self.inspector_master: Optional[pd.DataFrame] = None
        self.scheduled_products: Optional[pd.DataFrame] = None

    def load_data(self) -> bool:
        """
        全データを読み込む
        Returns:
            bool: 読み込み成功フラグ
        """
        logger.info("データ読み込みを開始します")

        self.shortage_data, self.product_master, self.inspector_master = self.data_loader.load_all_data()

        if self.shortage_data is None or self.product_master is None:
            logger.error("必須データの読み込みに失敗しました")
            return False

        # データ検証
        if not self.data_loader.validate_data(self.shortage_data, self.product_master):
            logger.error("データの整合性チェックに失敗しました")
            return False

        logger.info("データ読み込みが完了しました")
        return True

    def calculate_schedules(self) -> pd.DataFrame:
        """
        検査スケジュールを計算
        Returns:
            DataFrame: スケジュール計算結果
        """
        if self.shortage_data is None or self.product_master is None:
            logger.error("データが読み込まれていません")
            return pd.DataFrame()

        logger.info("検査スケジュール計算を開始します")

        # 時間計算を追加
        self.scheduled_products = self.date_calculator.add_time_calculations(
            self.shortage_data, self.product_master
        )

        # データが空の場合の処理
        if self.scheduled_products.empty:
            logger.info("スケジュール対象の製品がありません")
            return pd.DataFrame()

        # 必要な列の存在確認
        if '検査開始期限' not in self.scheduled_products.columns:
            logger.error("検査開始期限列が見つかりません")
            return pd.DataFrame()

        # 期限切れや無効なデータを除外
        valid_products = self.scheduled_products[
            self.scheduled_products['検査開始期限'].notna()
        ].copy()

        # 緊急度順でソート
        valid_products = valid_products.sort_values([
            '緊急度レベル', '期限までの日数', '総検査時間'
        ], ascending=[True, True, False])

        logger.info(f"検査スケジュール計算が完了しました: {len(valid_products)}件")
        return valid_products

    def get_urgent_products(self, max_days: int = 3) -> pd.DataFrame:
        """
        緊急対応が必要な製品を取得
        Args:
            max_days: 緊急と判定する日数
        Returns:
            DataFrame: 緊急対応製品リスト
        """
        if self.scheduled_products is None:
            logger.warning("スケジュール計算が実行されていません")
            return pd.DataFrame()

        urgent_products = self.date_calculator.filter_urgent_products(
            self.scheduled_products, max_days
        )

        return urgent_products

    def analyze_inspector_capacity(self) -> Dict:
        """
        検査員の作業キャパシティを分析
        Returns:
            Dict: キャパシティ分析結果
        """
        if self.inspector_master is None:
            return {"error": "検査員マスタが読み込まれていません"}

        # 検査員の基本情報を集計
        total_inspectors = len(self.inspector_master)

        # グループ別集計
        group_counts = {}
        if '所属グループ' in self.inspector_master.columns:
            group_counts = self.inspector_master['所属グループ'].value_counts().to_dict()

        # 勤務時間分析
        working_hours_analysis = {}
        if '開始時刻' in self.inspector_master.columns and '終了時刻' in self.inspector_master.columns:
            # 時刻形式の処理（簡易版）
            try:
                self.inspector_master['勤務時間'] = self.inspector_master.apply(
                    lambda row: self._calculate_working_hours(row['開始時刻'], row['終了時刻']),
                    axis=1
                )
                working_hours_analysis = {
                    '平均勤務時間': self.inspector_master['勤務時間'].mean(),
                    '最大勤務時間': self.inspector_master['勤務時間'].max(),
                    '最小勤務時間': self.inspector_master['勤務時間'].min()
                }
            except Exception as e:
                logger.warning(f"勤務時間分析でエラー: {e}")

        capacity_analysis = {
            '総検査員数': total_inspectors,
            'グループ別人数': group_counts,
            '勤務時間分析': working_hours_analysis
        }

        return capacity_analysis

    def _calculate_working_hours(self, start_time: str, end_time: str) -> float:
        """
        勤務時間を計算（簡易版）
        Args:
            start_time: 開始時刻
            end_time: 終了時刻
        Returns:
            float: 勤務時間（時間）
        """
        try:
            # 時刻が同じ場合は0時間として処理
            if start_time == end_time:
                return 0.0

            # HH:MM形式を想定
            start_hour, start_min = map(int, start_time.split(':'))
            end_hour, end_min = map(int, end_time.split(':'))

            start_minutes = start_hour * 60 + start_min
            end_minutes = end_hour * 60 + end_min

            # 終了時刻が開始時刻より早い場合（翌日にまたがる場合）
            if end_minutes < start_minutes:
                end_minutes += 24 * 60

            working_minutes = end_minutes - start_minutes
            return working_minutes / 60.0

        except Exception as e:
            logger.warning(f"時刻解析エラー ({start_time} - {end_time}): {e}")
            return 8.0  # デフォルト8時間

    def get_schedule_summary(self) -> Dict:
        """
        スケジュール概要を取得
        Returns:
            Dict: スケジュール概要
        """
        if self.scheduled_products is None or self.scheduled_products.empty:
            return {
                '総製品数': 0,
                '期限超過製品数': 0,
                '平均期限までの日数': 0,
                '検査員別製品数': {}
            }
        
        # '期限までの日数'列の存在確認
        if '期限までの日数' not in self.scheduled_products.columns:
            self.logger.warning("'期限までの日数'列が存在しません")
            return {
                '総製品数': len(self.scheduled_products),
                '期限超過製品数': 0,
                '平均期限までの日数': 0,
                '検査員別製品数': self.scheduled_products.get('検査員', pd.Series()).value_counts().to_dict() if '検査員' in self.scheduled_products.columns else {}
            }
        
        # 期限超過製品数を計算
        overdue_count = len(self.scheduled_products[self.scheduled_products['期限までの日数'] < 0])
        
        # 平均期限までの日数を計算
        avg_days = self.scheduled_products['期限までの日数'].mean()
        
        # 検査員別製品数を計算
        inspector_counts = self.scheduled_products['検査員'].value_counts().to_dict() if '検査員' in self.scheduled_products.columns else {}
        
        return {
            '総製品数': len(self.scheduled_products),
            '期限超過製品数': overdue_count,
            '平均期限までの日数': round(avg_days, 1) if not pd.isna(avg_days) else 0,
            '検査員別製品数': inspector_counts
        }

    def generate_priority_list(self, limit: Optional[int] = None) -> pd.DataFrame:
        """
        優先順位リストを生成
        Args:
            limit: 取得件数上限
        Returns:
            DataFrame: 優先順位付き製品リスト
        """
        if self.scheduled_products is None or self.scheduled_products.empty:
            logger.error("スケジュール計算が実行されていないか、データが空です")
            return pd.DataFrame()

        # 必要な列を選択
        priority_columns = [
            '品番', '納期', '不足数', '検査時間', '総検査時間',
            '検査開始期限', '期限までの日数', '緊急度', '緊急度レベル'
        ]

        available_columns = [col for col in priority_columns if col in self.scheduled_products.columns]
        priority_list = self.scheduled_products[available_columns].copy()

        # 上限適用
        if limit:
            priority_list = priority_list.head(limit)

        return priority_list

    def run_full_analysis(self) -> Tuple[pd.DataFrame, Dict, Dict]:
        """
        完全な分析を実行
        Returns:
            Tuple: (優先製品リスト, スケジュール概要, キャパシティ分析)
        """
        logger.info("完全分析を開始します")

        # データ読み込み
        if not self.load_data():
            return pd.DataFrame(), {}, {}

        # スケジュール計算
        scheduled_products = self.calculate_schedules()

        # 各種分析
        schedule_summary = self.get_schedule_summary()
        capacity_analysis = self.analyze_inspector_capacity()

        # 緊急製品リスト
        urgent_products = self.get_urgent_products()

        logger.info("完全分析が完了しました")

        return urgent_products, schedule_summary, capacity_analysis

    def assign_inspectors(self) -> pd.DataFrame:
        """
        検査員を製品に割り当てる（優先順位に基づき）
        優先順位:
        1. 納期が本日
        2. 今日以降だが、1日では終わらない量の物
        3. 納期が1日後, 2日後...
        割当ロジック:
        - 1人で作業可能なタスク(<=8h)は、空き時間がある検査員に割り当てる
        - 複数人が必要なタスク(>8h)は、1日作業できる検査員を必要人数分割り当てる
        - 検査員の残り時間は、次のタスクに引き継がれる
        Returns:
            DataFrame: 割当結果（品番/工程/納期/総検査時間/必要人数/割当人数/不足人員/割当メンバー）
        """
        if self.scheduled_products is None or self.scheduled_products.empty:
            logger.error("スケジュール計算が実行されていません")
            return pd.DataFrame()
        if self.inspector_master is None or self.inspector_master.empty:
            logger.error("検査員マスタが読み込まれていません")
            return pd.DataFrame()

        inspectors = self.inspector_master.copy()

        # 勤務時間（時間）を計算（列があれば）
        avg_working_hours = 8.0
        try:
            if '開始時刻' in inspectors.columns and '終了時刻' in inspectors.columns:
                inspectors['勤務時間'] = inspectors.apply(
                    lambda row: self._calculate_working_hours(row['開始時刻'], row['終了時刻']), axis=1
                )
                if inspectors['勤務時間'].gt(0).any():
                    avg_working_hours = inspectors['勤務時間'].mean()
        except Exception as e:
            logger.warning(f"検査員の勤務時間計算に失敗しました: {e}（既定 {avg_working_hours}h を使用）")

        # 割当可能な検査員名リストを作成
        name_col = '氏名' if '氏名' in inspectors.columns else (inspectors.columns[0] if len(inspectors.columns) > 0 else None)
        if not name_col:
            logger.error("検査員名の列が特定できません")
            return pd.DataFrame()
        
        # 検査員のステータスを初期化（名前と利用可能時間）
        initial_inspectors = inspectors[name_col].dropna().astype(str).tolist()
        inspectors_status = [{'name': name, 'available_time': avg_working_hours} for name in initial_inspectors]

        products = self.scheduled_products.copy()
        
        # --- 優先順位付けのための前処理 ---
        from math import ceil

        # 1. 各タスクの本来の必要人数を計算（検査時間の妥当性チェック付き）
        def calculate_required_people(total_inspection_time, due_date):
            """修正された必要人数計算"""
            if not total_inspection_time or float(total_inspection_time) <= 0:
                return 1
            
            # 検査時間の妥当性チェック
            inspection_time = float(total_inspection_time)
            
            # 納期までの日数を考慮
            today = self.date_calculator.base_date.date()
            if isinstance(due_date, str):
                due_date_obj = pd.to_datetime(due_date, errors='coerce')
                if pd.isna(due_date_obj):
                    days_until_due = 1
                else:
                    days_until_due = (due_date_obj.date() - today).days
            else:
                days_until_due = 1
            
            # 最低1日は確保
            if days_until_due <= 0:
                days_until_due = 1
            
            # 利用可能な総作業時間
            available_hours = days_until_due * max(avg_working_hours, 0.1)
            
            # 必要人数の計算
            required_people = ceil(inspection_time / available_hours)
            
            # 現実的な人数制限（最大50人）
            if required_people > 50:
                required_people = 50
            
            # 最低1人は必要
            if required_people < 1:
                required_people = 1
            
            return required_people

        products['必要人数'] = products.apply(
            lambda row: calculate_required_people(row['総検査時間'], row['納期']), axis=1
        )

        # 2. 優先度を判定する列を追加
        today = self.date_calculator.base_date.date()
        due_dates = pd.to_datetime(products['納期'], errors='coerce').dt.date
        products['due_date_diff'] = (due_dates - today).apply(lambda x: x.days if pd.notna(x) else 999)
        products['is_due_today'] = (due_dates == today)
        # 本日分は「一人で処理可能」を先に処理する
        products['priority_group'] = products.apply(
            lambda r: 0 if r['is_due_today'] and r['必要人数'] == 1
            else (1 if r['is_due_today'] and r['必要人数'] > 1
            else (2 if (not r['is_due_today']) and r['必要人数'] > 1
            else 3)),
            axis=1
        )

        # 3. 優先順位に基づきソート
        products = products.sort_values(
            by=['priority_group', 'due_date_diff', '総検査時間'],
            ascending=[True, True, False],
            na_position='last'
        ).reset_index(drop=True)
        
        # --- 割当処理 ---
        results = []
        for _, row in products.iterrows():
            task_time = float(row.get('総検査時間', 0) or 0)
            required = int(row.get('必要人数', 0))
            
            assigned_names = []
            assigned_count = 0

            if task_time > 0:
                # 検査員を利用可能時間が多い順にソート（割当の安定化のため）
                inspectors_status.sort(key=lambda x: x['available_time'], reverse=True)

                if required == 1:
                    # 1人で可能なタスク：タスク時間以上の空きがある検査員を探す
                    for inspector in inspectors_status:
                        if inspector['available_time'] >= task_time:
                            assigned_names.append(inspector['name'])
                            inspector['available_time'] -= task_time
                            assigned_count = 1
                            break
                else:
                    # 複数人必要なタスク：1日(avg_working_hours)作業できる人を必要人数分探す
                    eligible_inspectors = [i for i in inspectors_status if i['available_time'] >= avg_working_hours]

                    # 必要人数は「総検査時間 ÷ avg_working_hours」を上限にする（人数を確保できても作業量以上は不要）
                    from math import ceil
                    required_by_volume = max(1, ceil(task_time / max(avg_working_hours, 0.1)))
                    effective_required = min(required, required_by_volume)

                    # 確保できる人数分だけ割り当てる
                    assignable_count = min(effective_required, len(eligible_inspectors))
                    if assignable_count > 0:
                        assigned_inspectors = eligible_inspectors[:assignable_count]
                        assigned_names = [i['name'] for i in assigned_inspectors]
                        for inspector in assigned_inspectors:
                            inspector['available_time'] -= avg_working_hours
                        assigned_count = assignable_count

            item = {
                '品番': row.get('品番'),
                '工程番号': row.get('工程番号'),
                '納期': row.get('納期'),
                '総検査時間': task_time,
                '必要人数': required,
                '割当人数': assigned_count,
                '不足人員': max((int(max(1, __import__("math").ceil(task_time / max(avg_working_hours, 0.1)))) if required > 1 else required) - assigned_count, 0),
                '割当メンバー': ','.join(assigned_names) if assigned_names else ''
            }
            results.append(item)

        return pd.DataFrame(results)