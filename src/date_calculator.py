"""
日付・期間計算モジュール
検査開始期限の計算と緊急度判定を行う
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)

class DateCalculator:
    """日付計算クラス"""

    def __init__(self, base_date: Optional[datetime] = None):
        """
        初期化
        Args:
            base_date: 基準日（現在日時）。Noneの場合は現在日時を使用
        """
        self.base_date = base_date or datetime.now()

    def calculate_inspection_deadline(self, due_date: datetime, inspection_hours: float) -> datetime:
        """
        検査開始期限を計算
        Args:
            due_date: 納期
            inspection_hours: 検査時間（時間）
        Returns:
            datetime: 検査開始期限
        """
        # 検査時間を日数に変換（1日8時間稼働と仮定）
        inspection_days = inspection_hours / 8.0

        # 営業日のみを考慮した逆算（土日を除く）
        deadline = due_date
        remaining_days = inspection_days

        while remaining_days > 0:
            deadline -= timedelta(days=1)
            # 土日をスキップ（平日のみカウント）
            if deadline.weekday() < 5:  # 0-4 = 月-金
                remaining_days -= 1

        return deadline

    def calculate_urgency_level(self, inspection_deadline: datetime) -> int:
        """
        緊急度レベルを計算
        Args:
            inspection_deadline: 検査開始期限
        Returns:
            int: 緊急度レベル（1=最緊急、2=緊急、3=注意、4=通常）
        """
        days_until_deadline = (inspection_deadline - self.base_date).days

        if days_until_deadline <= 1:
            return 1  # 1日以内 = 最緊急
        elif days_until_deadline <= 3:
            return 2  # 3日以内 = 緊急
        elif days_until_deadline <= 7:
            return 3  # 1週間以内 = 注意
        else:
            return 4  # 通常

    def get_urgency_description(self, level: int) -> str:
        """
        緊急度レベルの説明を取得
        Args:
            level: 緊急度レベル
        Returns:
            str: 緊急度の説明
        """
        descriptions = {
            1: "最緊急（1日以内）",
            2: "緊急（3日以内）",
            3: "注意（1週間以内）",
            4: "通常"
        }
        return descriptions.get(level, "不明")

    def filter_urgent_products(self, products_df: pd.DataFrame, max_days: int = 3) -> pd.DataFrame:
        """
        指定日数以内に検査開始が必要な製品をフィルタリング
        Args:
            products_df: 製品データフレーム（検査開始期限を含む）
            max_days: 最大日数
        Returns:
            DataFrame: フィルタリング後の製品データ
        """
        # データが空の場合の処理
        if products_df.empty:
            logger.info("製品データが空のため、空のDataFrameを返します")
            return pd.DataFrame()
        
        if '検査開始期限' not in products_df.columns:
            logger.error("検査開始期限列が見つかりません")
            return pd.DataFrame()

        # 期限までの日数を計算（現在日から検査開始期限までの日数）
        products_df['期限までの日数'] = products_df['検査開始期限'].apply(
            lambda x: (x - self.base_date).days if pd.notna(x) else 999
        )

        # 指定日数以内の製品をフィルタリング
        urgent_products = products_df[products_df['期限までの日数'] <= max_days].copy()

        logger.info(f"緊急対応が必要な製品: {len(urgent_products)}件 (基準: {max_days}日以内)")
        return urgent_products

    def add_time_calculations(self, shortage_df: pd.DataFrame, product_master_df: pd.DataFrame) -> pd.DataFrame:
        """
        出荷不足データに時間計算結果を追加（ロット単位考慮）
        Args:
            shortage_df: 出荷不足データ（ロット情報含む）
            product_master_df: 製品マスタデータ
        Returns:
            DataFrame: 時間計算結果を追加したデータ
        """
        # データが空の場合の処理
        if shortage_df.empty:
            logger.info("出荷不足データが空のため、空のDataFrameを返します")
            return pd.DataFrame()
        
        if product_master_df.empty:
            logger.warning("製品マスタが空です")
            return pd.DataFrame()
        
        # 必要な列の存在確認
        required_shortage_cols = ['品番', '納期', '不足数']
        required_master_cols = ['品番', '工程番号', '検査時間']
        
        missing_shortage_cols = [col for col in required_shortage_cols if col not in shortage_df.columns]
        missing_master_cols = [col for col in required_master_cols if col not in product_master_df.columns]
        
        if missing_shortage_cols:
            logger.error(f"出荷不足データに必要な列が不足しています: {missing_shortage_cols}")
            return pd.DataFrame()
        
        if missing_master_cols:
            logger.error(f"製品マスタに必要な列が不足しています: {missing_master_cols}")
            return pd.DataFrame()

        # 製品マスタとマージ（品番と工程番号の複合キーで結合）
        # 製品マスタから工程別の検査時間を整形
        master_subset = product_master_df[['品番', '工程番号', '検査時間']].copy()
        master_subset['品番'] = master_subset['品番'].astype(str).str.strip()
        master_subset['検査時間'] = pd.to_numeric(master_subset['検査時間'], errors='coerce')

        def normalize_process_no(value):
            if pd.isna(value):
                return None
            if isinstance(value, str):
                stripped = value.strip()
                if stripped == '':
                    return None
                try:
                    num = float(stripped)
                    if pd.isna(num):
                        return None
                    if num.is_integer():
                        return str(int(num))
                    return str(num)
                except ValueError:
                    return stripped
            try:
                num = float(value)
            except (TypeError, ValueError):
                return str(value)
            if pd.isna(num):
                return None
            if num.is_integer():
                return str(int(num))
            return str(num)

        master_subset['工程番号標準'] = master_subset['工程番号'].apply(normalize_process_no)
        defined_subset = master_subset[master_subset['工程番号標準'].notna()].copy()
        undefined_subset = master_subset[master_subset['工程番号標準'].isna()].copy()
        products_with_defined = set(defined_subset['品番'])
        undefined_subset = undefined_subset[~undefined_subset['品番'].isin(products_with_defined)].copy()
        undefined_subset['工程番号標準'] = '未設定'

        process_master = pd.concat([defined_subset, undefined_subset], ignore_index=True)
        process_master = process_master.dropna(subset=['検査時間'])
        process_master = process_master.groupby(['品番', '工程番号標準'], as_index=False)['検査時間'].mean()

        def build_process_list(series):
            ordered = []
            for item in series:
                if pd.isna(item):
                    continue
                if item not in ordered:
                    ordered.append(item)
            if not ordered:
                return ''
            return ','.join(ordered)

        process_list_map = process_master.groupby('品番')['工程番号標準'].apply(build_process_list)

        result_df = shortage_df.copy()
        # 同一納期・品番・工程で複数ロットがある場合は不足数の絶対値が最大の行のみ採用
        if {'品番', '納期', '工程番号', '不足数'}.issubset(result_df.columns):
            result_df = result_df.copy()
            result_df['__不足数_abs'] = result_df['不足数'].abs()
            result_df = result_df.sort_values('__不足数_abs', ascending=False) \
                .drop_duplicates(subset=['品番', '納期', '工程番号'], keep='first')
            result_df = result_df.drop(columns='__不足数_abs')

        if '工程番号' in result_df.columns:
            result_df = result_df.rename(columns={'工程番号': '現在工程番号'})
        else:
            result_df['現在工程番号'] = None

        result_df['現在工程番号標準'] = result_df['現在工程番号'].apply(normalize_process_no)

        result_df = result_df.merge(
            process_master,
            left_on=['品番', '現在工程番号標準'],
            right_on=['品番', '工程番号標準'],
            how='left')

        result_df['工程番号一覧'] = result_df['品番'].map(process_list_map).fillna('')
        result_df['工程番号'] = result_df['現在工程番号']
        result_df['工程番号'] = result_df['工程番号'].where(result_df['工程番号'].notna(), '未登録')
        result_df['工程番号一覧'] = result_df['工程番号一覧'].where(result_df['工程番号一覧'] != '', result_df['工程番号'])
        result_df = result_df.drop(columns=['工程番号標準', '現在工程番号標準'], errors='ignore')
        missing_inspection_time = result_df['検査時間'].isna()
        if missing_inspection_time.any():
            process_series = product_master_df['工程番号']
            blank_process_mask = process_series.isna() | process_series.astype(str).str.strip().eq('')
            fallback_master = product_master_df.loc[blank_process_mask, ['品番', '検査時間']].dropna(subset=['検査時間'])
            if not fallback_master.empty:
                fallback_map = fallback_master.groupby('品番')['検査時間'].mean()
                fallback_series = result_df.loc[missing_inspection_time, '品番'].map(fallback_map)
                fill_indices = fallback_series.dropna().index
                if len(fill_indices) > 0:
                    result_df.loc[fill_indices, '検査時間'] = fallback_series.loc[fill_indices]
                    logger.info(f"工程番号が未設定の製品マスタ値で検査時間を補完しました: {len(fill_indices)}件")

        missing_inspection_time = result_df['検査時間'].isna()
        if missing_inspection_time.any():
            logger.info(f"検査時間が不明な製品が{missing_inspection_time.sum()}件あります。工程番号0の検査時間でフォールバック処理を実行します。")
            
            # 工程番号0（共通工程）の検査時間を取得
            process_0_master = product_master_df[product_master_df['工程番号'] == 0][['品番', '検査時間']]
            
            # 工程番号0の検査時間で埋める
            for idx in result_df[missing_inspection_time].index:
                part_number = result_df.loc[idx, '品番']
                process_0_time = process_0_master[process_0_master['品番'] == part_number]['検査時間']
                
                if not process_0_time.empty:
                    result_df.loc[idx, '検査時間'] = process_0_time.iloc[0]
                    logger.debug(f"品番{part_number}の検査時間を工程番号0から取得: {process_0_time.iloc[0]:.6f}時間")

        # それでも検査時間が不明な製品はデフォルト値を適用
        still_missing = result_df['検査時間'].isna()
        if still_missing.any():
            result_df['検査時間'] = result_df['検査時間'].fillna(2.0)
            logger.warning(f"検査時間が完全に不明な製品が{still_missing.sum()}件あり、デフォルト値2.0時間を適用しました。")
        result_df['工程番号'] = result_df['工程番号'].fillna('未登録')
        result_df['工程番号一覧'] = result_df['工程番号一覧'].where(result_df['工程番号一覧'] != '', result_df['工程番号'])

        # 総検査時間の計算に使用する数量を決定
        # 'ロット総数量'があればそれを使い、なければ'不足数'の絶対値を使う
        if 'ロット総数量' in result_df.columns:
            quantity = pd.to_numeric(result_df['ロット総数量'], errors='coerce').fillna(0)
            logger.info("「ロット総数量」を使用して総検査時間を計算します。")
        else:
            if '不足数' not in result_df.columns:
                logger.error("'不足数'列が見つかりません")
                return pd.DataFrame()
            quantity = pd.to_numeric(result_df['不足数'], errors='coerce').fillna(0).abs()
            neg_count = (pd.to_numeric(result_df['不足数'], errors='coerce').fillna(0) < 0).sum()
            if neg_count > 0:
                logger.info(f"不足数に負数が {neg_count} 件あり、絶対値で総検査時間を計算します")
            logger.info("「不足数」の絶対値を使用して総検査時間を計算します。")
        
        # '実生産数量'として計算に使用した数量を記録
        result_df['実生産数量'] = quantity
        
        # 総検査時間を計算 (数量 * 1個あたりの検査時間[h])
        result_df['総検査時間'] = result_df['実生産数量'] * result_df['検査時間']


        # 検査開始期限を計算（総検査時間ベース）
        result_df['検査開始期限'] = result_df.apply(
            lambda row: self.calculate_inspection_deadline(row['納期'], row['総検査時間'])
            if pd.notna(row['納期']) else None,
            axis=1
        )

        # 緊急度レベルを計算
        result_df['緊急度レベル'] = result_df['検査開始期限'].apply(
            lambda x: self.calculate_urgency_level(x) if pd.notna(x) else 4
        )

        # 緊急度説明を追加
        result_df['緊急度'] = result_df['緊急度レベル'].apply(self.get_urgency_description)

        # 期限までの日数を追加（現在日から検査開始期限までの日数）
        result_df['期限までの日数'] = result_df['検査開始期限'].apply(
            lambda x: (x - self.base_date).days if pd.notna(x) else 999
        )

        return result_df

    def get_workday_count(self, start_date: datetime, end_date: datetime) -> int:
        """
        指定期間の営業日数を計算
        Args:
            start_date: 開始日
            end_date: 終了日
        Returns:
            int: 営業日数
        """
        if start_date >= end_date:
            return 0

        current_date = start_date
        workdays = 0

        while current_date < end_date:
            # 平日（月-金）のみカウント
            if current_date.weekday() < 5:
                workdays += 1
            current_date += timedelta(days=1)

        return workdays

    def get_production_schedule_summary(self, products_df: pd.DataFrame) -> Dict:
        """
        生産スケジュール概要を取得
        Args:
            products_df: 製品データフレーム
        Returns:
            Dict: スケジュール概要
        """
        # データが空の場合の処理
        if products_df.empty:
            return {
                '総製品数': 0,
                '最緊急': 0,
                '緊急': 0,
                '注意': 0,
                '通常': 0,
                '総検査時間': 0,
                '平均検査時間': 0
            }

        summary = {
            '総製品数': len(products_df),
            '総検査時間': products_df['総検査時間'].sum() if '総検査時間' in products_df.columns else 0,
            '平均検査時間': products_df['検査時間'].mean() if '検査時間' in products_df.columns else 0
        }

        # 緊急度レベル列の存在確認
        if '緊急度レベル' in products_df.columns:
            summary.update({
                '最緊急': len(products_df[products_df['緊急度レベル'] == 1]),
                '緊急': len(products_df[products_df['緊急度レベル'] == 2]),
                '注意': len(products_df[products_df['緊急度レベル'] == 3]),
                '通常': len(products_df[products_df['緊急度レベル'] == 4])
            })
        else:
            summary.update({
                '最緊急': 0,
                '緊急': 0,
                '注意': 0,
                '通常': 0
            })

        return summary