from __future__ import annotations

import csv
import tempfile
import unittest
from pathlib import Path
from zipfile import ZipFile

from data_processing.combine_extracted_klines_logic import CombineConfig, combine_extracted_klines
from data_processing.kline_constants import KLINE_COLUMNS
from data_processing.process_klines_monthly_logic import ExtractConfig, extract_monthly_klines

try:
    import pyarrow.parquet as pq
except ImportError:  # pragma: no cover - optional dependency during local editing
    pq = None


BASE_OPEN_US = 1_759_276_800_000_000


def make_kline_row(
    open_time_us: int,
    *,
    open_price: str = "1.0",
    high_price: str = "2.0",
    low_price: str = "0.5",
    close_price: str = "1.5",
    volume: str = "10",
    quote_volume: str = "5",
    trades: str = "7",
    taker_base_volume: str = "3",
    taker_quote_volume: str = "4",
    ignore: str = "0",
) -> list[str]:
    return [
        str(open_time_us),
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        str(open_time_us + 999_999),
        quote_volume,
        trades,
        taker_base_volume,
        taker_quote_volume,
        ignore,
    ]


class KlineProcessingTests(unittest.TestCase):
    def test_extract_monthly_klines_restores_header_for_each_member(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir_name:
            tmp_dir = Path(tmp_dir_name)
            source_dir = tmp_dir / "downloads" / "data" / "spot" / "monthly" / "klines" / "BTCUSDT" / "1s"
            output_dir = tmp_dir / "downloads" / "test1"
            source_dir.mkdir(parents=True)

            zip_path = source_dir / "BTCUSDT-1s-2025-10.zip"
            with ZipFile(zip_path, "w") as zf:
                member1 = "\n".join(
                    [
                        ",".join(make_kline_row(BASE_OPEN_US)),
                        ",".join(make_kline_row(BASE_OPEN_US + 1_000_000)),
                    ]
                ) + "\n"
                member2 = "\n".join(
                    [
                        ",".join(KLINE_COLUMNS),
                        ",".join(make_kline_row(BASE_OPEN_US + 2_000_000)),
                    ]
                ) + "\n"
                zf.writestr("part-a.csv", member1)
                zf.writestr("part-b.csv", member2)

            result = extract_monthly_klines(
                ExtractConfig(
                    symbol="BTCUSDT",
                    interval="1s",
                    project_root=tmp_dir,
                    source_dir=source_dir,
                    output_dir=output_dir,
                    overwrite_existing=True,
                    read_chunk_size=1,
                )
            )

            self.assertEqual(
                [path.name for path in result.extracted_csv_paths],
                ["BTCUSDT-1s-2025-10-part1.csv", "BTCUSDT-1s-2025-10-part2.csv"],
            )

            first_lines = result.extracted_csv_paths[0].read_text(encoding="utf-8").splitlines()
            second_lines = result.extracted_csv_paths[1].read_text(encoding="utf-8").splitlines()

            self.assertEqual(first_lines[0], ",".join(KLINE_COLUMNS))
            self.assertEqual(first_lines[1], ",".join(make_kline_row(BASE_OPEN_US)))
            self.assertEqual(first_lines[2], ",".join(make_kline_row(BASE_OPEN_US + 1_000_000)))

            self.assertEqual(second_lines[0], ",".join(KLINE_COLUMNS))
            self.assertEqual(len(second_lines), 2)
            self.assertEqual(second_lines[1], ",".join(make_kline_row(BASE_OPEN_US + 2_000_000)))

    def test_combine_extracted_klines_preserves_issue_accounting_and_converts_datetime(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir_name:
            tmp_dir = Path(tmp_dir_name)
            extracted_dir = tmp_dir / "downloads" / "extracted"
            output_dir = tmp_dir / "downloads" / "combined"
            extracted_dir.mkdir(parents=True)
            output_dir.mkdir(parents=True)

            csv1 = extracted_dir / "BTCUSDT-1s-2025-10.csv"
            csv1.write_text(
                "\n".join(
                    [
                        ",".join(KLINE_COLUMNS),
                        ",".join(make_kline_row(BASE_OPEN_US)),
                        "1,2",
                        ",".join(
                            make_kline_row(
                                BASE_OPEN_US + 1_000_000,
                                volume="-1",
                                trades="-3",
                            )
                        ),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            csv2 = extracted_dir / "BTCUSDT-1s-2025-11.csv"
            csv2.write_text(
                ",".join(make_kline_row(BASE_OPEN_US + 1_000_000)) + "\n",
                encoding="utf-8",
            )

            result = combine_extracted_klines(
                CombineConfig(
                    symbol="BTCUSDT",
                    interval="1s",
                    start_yyyy_mm="2025-10",
                    end_yyyy_mm="2025-11",
                    project_root=tmp_dir,
                    extracted_dir=extracted_dir,
                    output_dir=output_dir,
                    output_format="csv",
                    keep_columns=["Open time", "Open"],
                    column_renames={"Open time": "Datetime"},
                    datetime_columns_to_utc=["Datetime"],
                    read_chunk_size=2,
                )
            )

            self.assertEqual(result.rows_read, 4)
            self.assertEqual(result.rows_written, 3)
            self.assertEqual(result.files_processed, 2)
            self.assertEqual(result.issue_counts["missing_or_unexpected_header"], 1)
            self.assertEqual(result.issue_counts["malformed_row_column_count"], 1)
            self.assertEqual(result.issue_counts["negative_volume"], 1)
            self.assertEqual(result.issue_counts["negative_trade_count"], 1)
            self.assertEqual(result.issue_counts["timestamp_duplicate"], 1)

            with result.combined_output_path.open("r", encoding="utf-8", newline="") as src:
                rows = list(csv.reader(src))

            self.assertEqual(rows[0], ["Datetime", "Open"])
            self.assertEqual(rows[1], ["2025-10-01 00:00:00+00:00", "1.0"])
            self.assertEqual(rows[2], ["2025-10-01 00:00:01+00:00", "1.0"])
            self.assertEqual(rows[3], ["2025-10-01 00:00:01+00:00", "1.0"])

    @unittest.skipUnless(pq is not None, "pyarrow is required for parquet verification")
    def test_combine_extracted_klines_writes_utc_timestamp_parquet(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir_name:
            tmp_dir = Path(tmp_dir_name)
            extracted_dir = tmp_dir / "downloads" / "extracted"
            output_dir = tmp_dir / "downloads" / "combined"
            extracted_dir.mkdir(parents=True)
            output_dir.mkdir(parents=True)

            csv_path = extracted_dir / "BTCUSDT-1s-2025-10.csv"
            csv_path.write_text(
                "\n".join(
                    [
                        ",".join(KLINE_COLUMNS),
                        ",".join(make_kline_row(BASE_OPEN_US)),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            result = combine_extracted_klines(
                CombineConfig(
                    symbol="BTCUSDT",
                    interval="1s",
                    start_yyyy_mm="2025-10",
                    end_yyyy_mm="2025-10",
                    project_root=tmp_dir,
                    extracted_dir=extracted_dir,
                    output_dir=output_dir,
                    output_format="parquet",
                    keep_columns=["Open time", "Open"],
                    column_renames={"Open time": "Datetime"},
                    datetime_columns_to_utc=["Datetime"],
                    read_chunk_size=1,
                    parquet_chunk_size=1,
                )
            )

            table = pq.read_table(result.combined_output_path)
            self.assertEqual(str(table.schema.field("Datetime").type), "timestamp[us, tz=UTC]")
            self.assertEqual(table.column("Open").to_pylist(), [1.0])


if __name__ == "__main__":
    unittest.main()
