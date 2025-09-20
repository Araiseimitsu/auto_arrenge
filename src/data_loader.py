"""
データファイル読み込みモジュール
Excel/CSVファイルからデータを読み込み、処理しやすい形式に変換する
"""

import pandas as pd
from pathlib import Path
from typing import Tuple, Optional, Dict
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataLoader:
    """データ読み込みクラス"""

    def __init__(self, data_dir: str, config: Optional[Dict] = None):
        """
        初期化
        Args:
            data_dir: データディレクトリのパス
            config: 外部からの設定
        """
        self.data_dir = Path(data_dir)
        self.config = config if config is not None else {}
        self.file_paths = self._get_file_paths()

    def _get_file_paths(self) -> Dict[str, Path]:
        """
        データディレクトリ内の主要なファイルパスを取得する
        Returns:
            Dict[str, Path]: ファイル種別をキーとしたパスの辞書
        """
        return {
            "shortage_data": self.data_dir / "出荷不足20250919.xlsx",
            "product_master": self.data_dir / "製品マスタ.xlsx",
            "inspector_master": self.data_dir / "検査員マスタ.csv",
            "calendar": self.data_dir / "カレンダー.csv",
            "skill_master": self.data_dir / "スキルマスタ.csv",
        }

    def load_shortage_data(self, filename: str = "出荷不足20250919.xlsx") -> Optional[pd.DataFrame]:
        """
        出荷不足データを読み込む（ロット単位で処理）
        Args:
            filename: ファイル名
        Returns:
            DataFrame: 納期、品番、不足数、ロット情報を含むデータ
        """
        file_path = self.file_paths.get("shortage_data")
        if not file_path or not file_path.exists():
            logger.error(f"出荷不足データファイルが見つかりません: {file_path}")
            return None

        try:
            df = pd.read_excel(file_path, engine='openpyxl')
            # 列名を標準化
            df.columns = df.columns.astype(str)

            # 必要な列を取得 A=納期、B=品番、E=出荷数、H=不足数、I=生産ロットID、J=ロット数量（工程番号は製品マスタから取得）
            if len(df.columns) >= 10:
                shortage_data = df.iloc[:, [0, 1, 4, 7, 8, 9]].copy()  # A, B, E, H, I, J列
                shortage_data.columns = ['納期', '品番', '出荷数', '不足数', '生産ロットID', 'ロット数量']

                # データクリーニング
                shortage_data = shortage_data.dropna(subset=['品番'])
                # マージキーの型・表記統一
                shortage_data['品番'] = shortage_data['品番'].astype(str).str.strip()
                shortage_data['納期'] = pd.to_datetime(shortage_data['納期'], errors='coerce')
                shortage_data['出荷数'] = pd.to_numeric(shortage_data['出荷数'], errors='coerce')
                shortage_data['不足数'] = pd.to_numeric(shortage_data['不足数'], errors='coerce')
                shortage_data['ロット数量'] = pd.to_numeric(shortage_data['ロット数量'], errors='coerce')

                # 同じ品番・納期の組み合わせごとにグループ化して必要ロット数を計算
                processed_data = self._process_lot_requirements(shortage_data)

                logger.info(f"出荷不足データを読み込みました: {len(processed_data)}件")
                return processed_data
            else:
                logger.error("出荷不足データの列数が不足しています")
                return None

        except Exception as e:
            logger.error(f"出荷不足データの読み込みエラー: {e}")
            return None

    def _process_lot_requirements(self, shortage_data: pd.DataFrame) -> pd.DataFrame:
        """
        ロット単位での必要数を計算（出荷数の重複集計を回避）
        Args:
            shortage_data: 出荷不足データ
        Returns:
            DataFrame: ロット要件を含むデータ
        """
        result_list = []

        # 品番・納期でグループ化（全て別製品として扱う）
        for (due_date, product_code), group in shortage_data.groupby(['納期', '品番']):
            if pd.isna(due_date) or pd.isna(product_code):
                continue

            # 個別製品として処理（バリエーション集約なし）
            # 出荷数は重複を避けるため、ユニークな出荷予定日×品番の組み合わせで集計
            unique_shipments = group.groupby(['納期', '品番']).agg({
                '出荷数': 'first',  # 同じ納期・品番の出荷数は同じなので最初の値を使用
                '不足数': 'min'     # 最終的な不足数（最もマイナスの値）
            }).reset_index()
            
            total_shipment = unique_shipments['出荷数'].sum()  # 実際の出荷予定数
            actual_shortage = unique_shipments['不足数'].min()  # 実際の不足数（符号付き）
            
            # 不足数の処理（マイナス値も含めて処理）
            if actual_shortage == 0:
                logger.info(f"品番 {product_code} 納期 {due_date}: 不足数が0のため検査対象外")
                continue
            
            # マイナス値の場合は絶対値を使用（不足している数量として扱う）
            if actual_shortage < 0:
                logger.info(f"品番 {product_code} 納期 {due_date}: 不足数 {actual_shortage} を絶対値 {abs(actual_shortage)} として処理")
                total_shortage = abs(actual_shortage)
            else:
                total_shortage = actual_shortage

            # ロット数量の累計を計算
            all_lots = group.copy()
            all_lots_sorted = all_lots.sort_values('ロット数量', ascending=False)
            cumulative_qty = all_lots_sorted['ロット数量'].cumsum()

            # 実際の不足数を満たすのに必要な最小限のロット数を特定（修正版）
            selected_lots = []
            cumulative_qty = 0
            
            for _, lot in all_lots_sorted.iterrows():
                if cumulative_qty >= total_shortage:
                    break
                selected_lots.append(lot)
                cumulative_qty += lot['ロット数量']
            
            # 最低1つのロットは必要
            if len(selected_lots) == 0 and len(all_lots_sorted) > 0:
                selected_lots = [all_lots_sorted.iloc[0]]
                cumulative_qty = selected_lots[0]['ロット数量']
            
            # 実際に使用する数量は不足数を超えない
            actual_quantity = min(cumulative_qty, total_shortage)
            
            # 必要ロット数を記録
            required_lot_count = len(selected_lots)
            total_lot_qty = actual_quantity  # 修正: 実際の必要数量を使用

            result_list.append({
                '納期': due_date,
                '品番': product_code,  # 品番をそのまま使用
                '出荷予定数': total_shipment,
                '不足数': total_shortage,
                '必要ロット数': required_lot_count,
                'ロット総数量': total_lot_qty,
                'ロット詳細': pd.DataFrame(selected_lots)[['品番', '生産ロットID', 'ロット数量']].to_dict('records') if selected_lots else []
            })

        return pd.DataFrame(result_list)

    def load_product_master(self, filename: str = "製品マスタ.xlsx") -> Optional[pd.DataFrame]:
        """
        製品マスタデータを読み込む
        Args:
            filename: ファイル名
        Returns:
            DataFrame: 品番(B列)、検査時間(E列)を含むデータ
        """
        file_path = self.file_paths.get("product_master")
        if not file_path or not file_path.exists():
            logger.error(f"製品マスタファイルが見つかりません: {file_path}")
            return None

        try:
            df = pd.read_excel(file_path, engine='openpyxl')
            df.columns = df.columns.astype(str)

            # B列=品番、D列=工程番号、E列=検査時間として処理
            if len(df.columns) >= 5:
                product_data = df.iloc[:, [1, 3, 4]].copy()  # B, D, E列
                product_data.columns = ['品番', '工程番号', '検査時間']

                # データクリーニング
                product_data = product_data.dropna(subset=['品番'])

                # 品番を文字列に変換（datetime型になっている場合のエラーを回避）
                product_data['品番'] = product_data['品番'].astype(str)
                product_data['品番'] = product_data['品番'].astype(str).str.strip()
                
                # 工程番号を整数(Int64)に統一（NaNは0）
                product_data['工程番号'] = (
                    pd.to_numeric(product_data['工程番号'], errors='coerce')
                      .fillna(0)
                      .astype('Int64')
                )
                product_data['検査時間'] = pd.to_numeric(product_data['検査時間'], errors='coerce')

                # NaN値を持つ行を除去
                product_data = product_data.dropna()

                if len(product_data) == 0:
                    logger.error("有効な製品マスタデータがありません")
                    return None

                # 検査時間の単位を自動判定して時間[h]へ正規化
                s = product_data['検査時間'].dropna()
                if s.empty:
                    logger.error("製品マスタの検査時間が空です")
                    # 空でもエラーとせず、後続処理に任せる
                
                s_pos = s[s >= 0]
                max_v = s_pos.max() if not s_pos.empty else None
                q95 = s_pos.quantile(0.95) if not s_pos.empty else None
                med = s_pos.median() if not s_pos.empty else None

                # 単位を強制する場合、configから読み込む
                forced_unit = self.config.get('product_master_time_unit')

                # 検査時間（時間）の正規化
                if '検査時間' in product_data.columns:
                    unit = 'not_processed'
                    try:
                        # 強制単位が指定されている場合
                        if forced_unit == 'seconds':
                            product_data['検査時間'] = pd.to_numeric(product_data['検査時間'], errors='coerce') / 3600.0
                            unit = 'seconds_forced'
                        elif forced_unit == 'minutes':
                            product_data['検査時間'] = pd.to_numeric(product_data['検査時間'], errors='coerce') / 60.0
                            unit = 'minutes_forced'
                        elif forced_unit == 'hours':
                            product_data['検査時間'] = pd.to_numeric(product_data['検査時間'], errors='coerce')
                            unit = 'hours_forced'
                        elif forced_unit == 'excel':
                            product_data['検査時間'] = pd.to_numeric(product_data['検査時間'], errors='coerce') * 24.0
                            unit = 'excel_day_to_hours_forced'
                        elif forced_unit is not None:
                            logger.warning(f"不明な強制単位が指定されました: {forced_unit}。自動判定を試みます。")
                            # 自動判定ロジックへフォールバック
                            if max_v is not None and max_v <= 1.5:
                                product_data['検査時間'] = product_data['検査時間'] * 24.0
                                unit = 'excel_day_to_hours_fallback'
                            elif q95 is not None and q95 <= 24 and med is not None and med <= 8:
                                unit = 'hours_fallback'
                            elif q95 is not None and q95 <= 600:
                                product_data['検査時間'] = product_data['検査時間'] / 60.0
                                unit = 'minutes_to_hours_fallback'
                            else:
                                product_data['検査時間'] = product_data['検査時間'] / 3600.0
                                unit = 'seconds_to_hours_fallback'
                        else:
                            # 修正された自動判定ロジック
                            unit = 'auto'
                            if max_v is not None and max_v <= 1.5:
                                product_data['検査時間'] = product_data['検査時間'] * 24.0
                                unit = 'excel_day_to_hours'
                            elif q95 is not None and q95 <= 100 and med is not None and med <= 60:
                                # 95パーセンタイルが100以下かつ中央値が60以下なら分単位と判定
                                # （通常の検査時間は数分～数十分程度のため）
                                product_data['検査時間'] = product_data['検査時間'] / 60.0
                                unit = 'minutes_to_hours'
                            elif q95 is not None and q95 <= 1.0 and med is not None and med <= 0.5:
                                # 非常に小さい値の場合は時間単位と判定
                                unit = 'hours'
                            else:
                                # 上記以外は秒単位として処理
                                product_data['検査時間'] = product_data['検査時間'] / 3600.0
                                unit = 'seconds_to_hours'
                    except Exception as e:
                        logger.error(f"検査時間の単位変換中にエラーが発生しました: {e}")
                        unit = 'error_in_conversion'

                    logger.info(f"製品マスタの検査時間を自動/固定判定しました: 単位={unit}（時間[h]に正規化済み）")

                # 同じ品番・工程番号で複数の検査時間がある場合の処理
                duplicate_products = product_data.groupby(['品番', '工程番号']).size()
                duplicates_found = duplicate_products[duplicate_products > 1]

                if len(duplicates_found) > 0:
                    logger.warning(f"製品マスタに重複品番・工程番号が{len(duplicates_found)}件見つかりました")

                    # 重複品番・工程番号の場合は平均検査時間を使用
                    product_data_dedup = product_data.groupby(['品番', '工程番号']).agg({
                        '検査時間': 'mean'  # 平均値を使用
                    }).reset_index()

                    logger.info(f"重複品番・工程番号を平均検査時間で統合しました")
                    logger.info(f"製品マスタを読み込みました: {len(product_data_dedup)}件（重複除去後）")
                    return product_data_dedup
                else:
                    logger.info(f"製品マスタを読み込みました: {len(product_data)}件")
                    return product_data
            else:
                logger.error("製品マスタの列数が不足しています")
                return None

        except Exception as e:
            logger.error(f"製品マスタの読み込みエラー: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None

    def load_inspector_master(self, filename: str = "検査員マスタ.csv") -> Optional[pd.DataFrame]:
        """
        検査員マスタデータを読み込む
        Args:
            filename: ファイル名
        Returns:
            DataFrame: 検査員情報
        """
        file_path = self.file_paths.get("inspector_master")
        if not file_path or not file_path.exists():
            logger.error(f"検査員マスタファイルが見つかりません: {file_path}")
            return None

        try:
            df = pd.read_csv(file_path, encoding='utf-8-sig')

            # 列名確認とクリーニング
            df.columns = df.columns.str.strip().str.replace('#', '')

            if '氏名' in df.columns:
                inspector_data = df.copy()
                logger.info(f"検査員マスタを読み込みました: {len(inspector_data)}件")
                return inspector_data
            else:
                logger.error("検査員マスタに氏名列が見つかりません")
                return None

        except Exception as e:
            logger.error(f"検査員マスタの読み込みエラー: {e}")
            return None

    def load_skill_master(self, filename: str = "スキルマスタ.csv") -> Optional[pd.DataFrame]:
        """
        スキルマスタデータを読み込む
        Args:
            filename: ファイル名
        Returns:
            DataFrame: 品番と各作業員のスキルレベル情報
        """
        file_path = self.file_paths.get("skill_master")
        if not file_path or not file_path.exists():
            logger.error(f"スキルマスタファイルが見つかりません: {file_path}")
            return None

        try:
            df = pd.read_csv(file_path, encoding='utf-8-sig')
            
            # 列名を確認してクリーニング
            df.columns = df.columns.str.strip()
            
            # A列（品番）が存在することを確認
            if len(df.columns) < 1:
                logger.error("スキルマスタの列数が不足しています")
                return None
            
            # 品番列（A列）を取得
            skill_data = df.copy()
            
            # 品番列の名前を統一
            skill_data.rename(columns={skill_data.columns[0]: '品番'}, inplace=True)
            
            # 品番のデータクリーニング
            skill_data = skill_data.dropna(subset=['品番'])
            skill_data['品番'] = skill_data['品番'].astype(str).str.strip()
            
            # 作業員列（C列以降）のスキルレベルを数値に変換
            worker_columns = skill_data.columns[2:]  # C列以降（作業員列）
            for col in worker_columns:
                # スキルレベルを数値に変換（1:高、2:中、3:低、空:割り振らない）
                skill_data[col] = pd.to_numeric(skill_data[col], errors='coerce')
            
            logger.info(f"スキルマスタを読み込みました: {len(skill_data)}件の品番、{len(worker_columns)}名の作業員")
            return skill_data
            
        except Exception as e:
            logger.error(f"スキルマスタの読み込みエラー: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None

    def load_all_data(self) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame], Optional[pd.DataFrame], Optional[pd.DataFrame]]:
        """
        全データファイルを読み込む
        Returns:
            Tuple: (出荷不足データ, 製品マスタ, 検査員マスタ, スキルマスタ)
        """
        shortage_data = self.load_shortage_data()
        product_master = self.load_product_master()
        inspector_master = self.load_inspector_master()
        skill_master = self.load_skill_master()

        return shortage_data, product_master, inspector_master, skill_master

    def get_process_and_inspection_time(self, shortage_data: pd.DataFrame, product_master: pd.DataFrame) -> pd.DataFrame:
        """
        出荷不足データと製品マスタを結合し、納期・品番をキーとして工程番号と検査時間を取得
        Args:
            shortage_data: 出荷不足データ（納期、品番を含む）
            product_master: 製品マスタ（品番、工程番号、検査時間を含む）
        Returns:
            DataFrame: 工程番号と検査時間が追加された出荷不足データ
        """
        if shortage_data is None or product_master is None:
            logger.error("データが不正です")
            return None
            
        if shortage_data.empty:
            logger.info("出荷不足データが空です")
            return shortage_data
            
        try:
            # 出荷不足データをコピー
            result_data = shortage_data.copy()
            
            # 製品マスタから品番ごとの工程番号と検査時間を取得
            # 同じ品番で複数の工程がある場合は全て取得
            merged_data = pd.merge(
                result_data,
                product_master[['品番', '工程番号', '検査時間']],
                on='品番',
                how='left'
            )
            
            # マージ結果の確認
            total_rows = len(merged_data)
            matched_rows = len(merged_data.dropna(subset=['工程番号', '検査時間']))
            
            logger.info(f"製品マスタとの結合結果: 全{total_rows}行中{matched_rows}行でマッチ")
            
            if matched_rows == 0:
                logger.warning("製品マスタとマッチする品番がありません")
                # 工程番号と検査時間の列を追加（NaN値で）
                result_data['工程番号'] = None
                result_data['検査時間'] = None
                return result_data
            
            # マッチしなかった品番をログ出力
            unmatched_data = merged_data[merged_data['工程番号'].isna()]
            if not unmatched_data.empty:
                unmatched_products = unmatched_data['品番'].unique()
                logger.warning(f"製品マスタに存在しない品番: {list(unmatched_products)[:10]}")
            
            # マッチしない場合のデフォルト値設定
            # 工程番号はNaN、検査時間は15秒（0.0042時間）をデフォルト値として設定
            DEFAULT_INSPECTION_TIME = 15 / 3600.0  # 15秒を時間に変換（0.0042時間）
            
            merged_data['工程番号'] = merged_data['工程番号'].fillna(pd.NA)
            merged_data['検査時間'] = merged_data['検査時間'].fillna(DEFAULT_INSPECTION_TIME)
            
            # デフォルト値が使用された品番数をログ出力
            default_used_count = len(merged_data[merged_data['検査時間'] == DEFAULT_INSPECTION_TIME])
            if default_used_count > 0:
                logger.info(f"製品マスタに存在しない品番{default_used_count}件にデフォルト検査時間15秒（0.0042時間）を設定しました")
            
            # 工程番号の型変換（「未登録」などの文字列をNaNに変換）
            def convert_process_number(value):
                if pd.isna(value) or value == '未登録' or str(value).strip() == '':
                    return pd.NA
                try:
                    return pd.to_numeric(value, errors='coerce')
                except:
                    return pd.NA
            
            merged_data['工程番号'] = merged_data['工程番号'].apply(convert_process_number)
            
            return merged_data
            
        except Exception as e:
            logger.error(f"工程番号・検査時間取得エラー: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None

    def validate_data(self, shortage_data: pd.DataFrame, product_master: pd.DataFrame) -> bool:
        """
        データの整合性をチェック
        Args:
            shortage_data: 出荷不足データ
            product_master: 製品マスタ
        Returns:
            bool: バリデーション結果
        """
        if shortage_data is None or product_master is None:
            return False

        try:
            # データが空の場合の処理
            if shortage_data.empty:
                logger.info("出荷不足データが空のため、検証をスキップします")
                return True  # 空データでも処理を続行
            
            if product_master.empty:
                logger.warning("製品マスタが空です")
                return False
            
            # '品番'列の存在確認
            if '品番' not in shortage_data.columns:
                logger.error("出荷不足データに'品番'列が存在しません")
                return False
            
            if '品番' not in product_master.columns:
                logger.error("製品マスタに'品番'列が存在しません")
                return False

            # 品番の整合性チェック
            shortage_products = set(shortage_data['品番'].dropna().astype(str))
            master_products = set(product_master['品番'].dropna().astype(str))

            missing_products = shortage_products - master_products
            if missing_products:
                logger.warning(f"製品マスタに存在しない品番: {list(missing_products)[:10]}")

            common_products = shortage_products & master_products
            logger.info(f"共通品番数: {len(common_products)}/{len(shortage_products)}")

            return len(common_products) > 0
        except Exception as e:
            logger.error(f"データ検証エラー: {e}")
            return True  # エラーが発生した場合は処理を続行