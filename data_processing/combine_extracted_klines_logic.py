"""Logic for combining extracted Binance kline CSV files with integrity checks."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
import csv
import itertools
import re

from data_processing.kline_constants import KLINE_COLUMNS
from data_processing.process_klines_monthly_logic import resolve_project_root


@dataclass
class CombineConfig:
    symbol: str = "BTCUSDT"
    interval: str = "1s"
    start_yyyy_mm: str = "2018-01"
    end_yyyy_mm: str = "2020-02"
    project_root: Path | None = None
    extracted_dir: Path | None = None
    output_dir: Path | None = None
    output_format: str = "csv"  # csv|parquet
    output_name: str = ""
    summary_log_name: str = ""
    keep_columns: list[str] = field(default_factory=list)
    drop_columns: list[str] = field(default_factory=list)
    parquet_compression: str = "snappy"
    parquet_chunk_size: int = 200_000
    max_examples_per_issue: int = 15


@dataclass
class CombineResult:
    project_root: Path
    extracted_dir: Path
    output_dir: Path
    combined_output_path: Path
    summary_log_path: Path
    selected_columns: list[str]
    selected_csvs: list[Path]
    files_processed: int
    rows_read: int
    rows_written: int
    issue_counts: dict[str, int]
    issue_examples: dict[str, list[str]]
    timestamp_units_observed: dict[str, int]
    expected_interval_us: int | None


def parse_yyyy_mm(text: str) -> datetime:
    return datetime.strptime(text, "%Y-%m")


def interval_to_us(interval: str) -> int | None:
    """Return expected bar size in microseconds; None for variable-size intervals."""
    mapping_seconds = {
        "1s": 1,
        "1m": 60,
        "3m": 3 * 60,
        "5m": 5 * 60,
        "15m": 15 * 60,
        "30m": 30 * 60,
        "1h": 60 * 60,
        "2h": 2 * 60 * 60,
        "4h": 4 * 60 * 60,
        "6h": 6 * 60 * 60,
        "8h": 8 * 60 * 60,
        "12h": 12 * 60 * 60,
        "1d": 24 * 60 * 60,
        "3d": 3 * 24 * 60 * 60,
        "1w": 7 * 24 * 60 * 60,
    }
    if interval == "1mo":
        return None
    sec = mapping_seconds.get(interval)
    if sec is None:
        raise ValueError(f"Unsupported interval for integrity checks: {interval}")
    return sec * 1_000_000


def parse_timestamp_to_us(raw_value: str) -> tuple[int | None, str]:
    """Normalize seconds/milliseconds/microseconds into microseconds."""
    ts = int(raw_value)
    if ts >= 10**15:
        return ts, "us"
    if ts >= 10**12:
        return ts * 1000, "ms"
    if ts >= 10**9:
        return ts * 1_000_000, "s"
    return None, "unknown"


def timestamp_unit_resolution_us(unit: str | None) -> int | None:
    mapping = {"us": 1, "ms": 1000, "s": 1_000_000}
    return mapping.get(unit) if unit is not None else None


def resolve_output_columns(
    all_columns: list[str],
    keep_columns: list[str],
    drop_columns: list[str],
) -> list[str]:
    keep = [col for col in keep_columns]
    drop = [col for col in drop_columns]

    unknown_keep = [col for col in keep if col not in all_columns]
    unknown_drop = [col for col in drop if col not in all_columns]
    if unknown_keep:
        raise ValueError(f"Unknown KEEP_COLUMNS entries: {unknown_keep}")
    if unknown_drop:
        raise ValueError(f"Unknown DROP_COLUMNS entries: {unknown_drop}")

    if keep and drop:
        raise ValueError("Use either KEEP_COLUMNS or DROP_COLUMNS, not both.")

    if keep:
        result = keep
    else:
        result = [col for col in all_columns if col not in set(drop)]

    if not result:
        raise ValueError("No columns selected.")
    if len(result) != len(set(result)):
        raise ValueError("Selected columns contain duplicates.")

    return result


def coerce_for_parquet(column_name: str, value: str):
    int_cols = {"Open time", "Close time", "Number of trades", "Ignore"}
    float_cols = {
        "Open",
        "High",
        "Low",
        "Close",
        "Volume",
        "Quote asset volume",
        "Taker buy base asset volume",
        "Taker buy quote asset volume",
    }
    if value == "" or value is None:
        return None
    if column_name in int_cols:
        try:
            return int(value)
        except ValueError:
            return None
    if column_name in float_cols:
        try:
            return float(value)
        except ValueError:
            return None
    return value


def default_extracted_dir(project_root: Path) -> Path:
    return project_root / "downloads" / "test1"


def select_extracted_csvs(
    extracted_dir: Path,
    symbol: str,
    interval: str,
    start_dt: datetime,
    end_dt: datetime,
) -> list[tuple[datetime, int, str, Path]]:
    csv_pattern = re.compile(
        rf"^{re.escape(symbol)}-{re.escape(interval)}-(\d{{4}}-\d{{2}})(?:-part(\d+))?\.csv$",
        flags=re.IGNORECASE,
    )

    selected = []
    for csv_path in extracted_dir.iterdir():
        if not csv_path.is_file() or csv_path.suffix.lower() != ".csv":
            continue
        match = csv_pattern.match(csv_path.name)
        if not match:
            continue
        month_text = match.group(1)
        part_text = match.group(2)
        part_num = int(part_text) if part_text else 1
        month_dt = parse_yyyy_mm(month_text)
        if start_dt <= month_dt <= end_dt:
            selected.append((month_dt, part_num, month_text, csv_path))

    selected.sort(key=lambda item: (item[0], item[1], item[3].name))
    return selected


def _build_summary_lines(
    config: CombineConfig,
    extracted_dir: Path,
    combined_output_path: Path,
    summary_log_path: Path,
    selected_columns: list[str],
    selected_csvs: list[tuple[datetime, int, str, Path]],
    files_processed: int,
    rows_total: int,
    rows_written: int,
    issue_counts: Counter,
    issue_examples: dict[str, list[str]],
    timestamp_units: Counter,
    expected_interval_us: int | None,
) -> list[str]:
    run_finished_at = datetime.now(timezone.utc)
    summary_lines = [
        "=== Kline Combine Integrity Summary ===",
        f"Generated at (UTC): {run_finished_at.isoformat()}",
        "",
        "[Config]",
        f"SYMBOL={config.symbol}",
        f"INTERVAL={config.interval}",
        f"START_YYYY_MM={config.start_yyyy_mm}",
        f"END_YYYY_MM={config.end_yyyy_mm}",
        f"OUTPUT_FORMAT={config.output_format.lower().strip()}",
        f"EXTRACTED_DIR={extracted_dir}",
        f"OUTPUT_PATH={combined_output_path}",
        f"SUMMARY_LOG_PATH={summary_log_path}",
        f"EXPECTED_INTERVAL_US={expected_interval_us}",
        f"SELECTED_COLUMNS={selected_columns}",
        "",
        "[Run Stats]",
        f"FILES_SELECTED={len(selected_csvs)}",
        f"FILES_PROCESSED={files_processed}",
        f"ROWS_READ={rows_total}",
        f"ROWS_WRITTEN={rows_written}",
        f"TIMESTAMP_UNITS_OBSERVED={dict(timestamp_units)}",
        "",
        "[Issue Counts]",
    ]

    if issue_counts:
        denom = rows_total if rows_total > 0 else 1
        for issue, count in issue_counts.most_common():
            pct = (count / denom) * 100
            summary_lines.append(f"{issue}: {count} ({pct:.6f}% of rows)")
    else:
        summary_lines.append("No integrity issues detected by implemented checks.")

    summary_lines.append("")
    summary_lines.append("[Issue Examples]")
    if issue_examples:
        for issue, examples in issue_examples.items():
            summary_lines.append(f"{issue}:")
            for ex in examples:
                summary_lines.append(f"  - {ex}")
    else:
        summary_lines.append("No issue examples.")

    return summary_lines


def combine_extracted_klines(config: CombineConfig) -> CombineResult:
    project_root = resolve_project_root(config.project_root)
    extracted_dir = config.extracted_dir or default_extracted_dir(project_root)
    output_dir = config.output_dir or extracted_dir

    start_dt = parse_yyyy_mm(config.start_yyyy_mm)
    end_dt = parse_yyyy_mm(config.end_yyyy_mm)
    if start_dt > end_dt:
        raise ValueError(
            f"start_yyyy_mm must be <= end_yyyy_mm ({config.start_yyyy_mm} > {config.end_yyyy_mm})"
        )

    if not extracted_dir.exists():
        raise FileNotFoundError(f"Extracted directory not found: {extracted_dir}")

    output_format = config.output_format.strip().lower()
    if output_format not in {"csv", "parquet"}:
        raise ValueError("output_format must be 'csv' or 'parquet'.")

    selected_columns = resolve_output_columns(KLINE_COLUMNS, config.keep_columns, config.drop_columns)
    selected_csvs = select_extracted_csvs(
        extracted_dir=extracted_dir,
        symbol=config.symbol,
        interval=config.interval,
        start_dt=start_dt,
        end_dt=end_dt,
    )
    if not selected_csvs:
        raise ValueError(
            "No extracted CSV files matched range "
            f"{config.start_yyyy_mm} to {config.end_yyyy_mm} in {extracted_dir}"
        )

    expected_interval_us = interval_to_us(config.interval)

    output_dir.mkdir(parents=True, exist_ok=True)
    auto_output_name = (
        f"{config.symbol}-{config.interval}-{config.start_yyyy_mm}_to_{config.end_yyyy_mm}-combined."
        f"{output_format}"
    )
    output_name = config.output_name if config.output_name else auto_output_name
    combined_output_path = output_dir / output_name

    auto_log_name = f"{combined_output_path.stem}-integrity-summary.txt"
    summary_log_name = config.summary_log_name if config.summary_log_name else auto_log_name
    summary_log_path = output_dir / summary_log_name

    issue_counts: Counter = Counter()
    issue_examples: defaultdict[str, list[str]] = defaultdict(list)
    timestamp_units: Counter = Counter()

    rows_total = 0
    rows_written = 0
    files_processed = 0

    def record_issue(issue_key: str, detail: str) -> None:
        issue_counts[issue_key] += 1
        if len(issue_examples[issue_key]) < config.max_examples_per_issue:
            issue_examples[issue_key].append(detail)

    csv_writer = None
    csv_handle = None
    parquet_writer = None
    parquet_schema = None
    parquet_buffer = None
    buffer_size = 0

    try:
        if output_format == "csv":
            csv_handle = combined_output_path.open("w", newline="", encoding="utf-8")
            csv_writer = csv.writer(csv_handle)
            csv_writer.writerow(selected_columns)
        else:
            try:
                import pyarrow as pa
                import pyarrow.parquet as pq
            except ImportError as exc:
                raise ImportError("Parquet output requires pyarrow. Install it, then rerun.") from exc

            parquet_field_types = []
            for col in selected_columns:
                if col in {"Open time", "Close time", "Number of trades", "Ignore"}:
                    parquet_field_types.append((col, pa.int64()))
                elif col in {
                    "Open",
                    "High",
                    "Low",
                    "Close",
                    "Volume",
                    "Quote asset volume",
                    "Taker buy base asset volume",
                    "Taker buy quote asset volume",
                }:
                    parquet_field_types.append((col, pa.float64()))
                else:
                    parquet_field_types.append((col, pa.string()))

            parquet_schema = pa.schema(parquet_field_types)
            parquet_writer = pq.ParquetWriter(
                str(combined_output_path),
                parquet_schema,
                compression=config.parquet_compression,
            )
            parquet_buffer = {col: [] for col in selected_columns}

        prev_open_time_us = None

        for _, _, _, csv_path in selected_csvs:
            files_processed += 1
            with csv_path.open("r", newline="", encoding="utf-8") as src:
                reader = csv.reader(src)
                first_row = next(reader, None)
                if first_row is None:
                    record_issue("empty_file", f"{csv_path.name}: file has no rows")
                    continue

                if first_row == KLINE_COLUMNS:
                    row_iter = reader
                else:
                    record_issue(
                        "missing_or_unexpected_header",
                        f"{csv_path.name}: first row is not expected header",
                    )
                    row_iter = itertools.chain([first_row], reader)

                for file_row_idx, row in enumerate(row_iter, start=1):
                    rows_total += 1
                    if len(row) != len(KLINE_COLUMNS):
                        record_issue(
                            "malformed_row_column_count",
                            f"{csv_path.name}:{file_row_idx} has {len(row)} columns (expected {len(KLINE_COLUMNS)})",
                        )
                        continue

                    row_map = dict(zip(KLINE_COLUMNS, row))

                    open_us = None
                    close_us = None
                    open_unit = None
                    close_unit = None

                    try:
                        open_us, open_unit = parse_timestamp_to_us(row_map["Open time"])
                        timestamp_units[open_unit] += 1
                        if open_us is None:
                            record_issue(
                                "open_time_unknown_unit",
                                f"{csv_path.name}:{file_row_idx} open_time={row_map['Open time']}",
                            )
                    except ValueError:
                        record_issue(
                            "open_time_parse_error",
                            f"{csv_path.name}:{file_row_idx} open_time={row_map['Open time']}",
                        )

                    try:
                        close_us, close_unit = parse_timestamp_to_us(row_map["Close time"])
                        timestamp_units[close_unit] += 1
                        if close_us is None:
                            record_issue(
                                "close_time_unknown_unit",
                                f"{csv_path.name}:{file_row_idx} close_time={row_map['Close time']}",
                            )
                    except ValueError:
                        record_issue(
                            "close_time_parse_error",
                            f"{csv_path.name}:{file_row_idx} close_time={row_map['Close time']}",
                        )

                    if open_us is not None and prev_open_time_us is not None:
                        delta = open_us - prev_open_time_us
                        if delta < 0:
                            record_issue("timestamp_decrease", f"{csv_path.name}:{file_row_idx} delta_us={delta}")
                        elif delta == 0:
                            record_issue(
                                "timestamp_duplicate",
                                f"{csv_path.name}:{file_row_idx} duplicate open time",
                            )
                        elif expected_interval_us is not None and delta != expected_interval_us:
                            record_issue(
                                "timestamp_step_mismatch",
                                f"{csv_path.name}:{file_row_idx} delta_us={delta}, expected={expected_interval_us}",
                            )
                            if delta > expected_interval_us:
                                record_issue(
                                    "timestamp_gap",
                                    f"{csv_path.name}:{file_row_idx} gap_us={delta - expected_interval_us}",
                                )
                            else:
                                record_issue(
                                    "timestamp_overlap",
                                    f"{csv_path.name}:{file_row_idx} overlap_us={expected_interval_us - delta}",
                                )

                    if open_us is not None:
                        prev_open_time_us = open_us

                    if open_unit is not None and close_unit is not None and open_unit != close_unit:
                        record_issue(
                            "open_close_time_unit_mismatch",
                            f"{csv_path.name}:{file_row_idx} open_unit={open_unit}, close_unit={close_unit}",
                        )

                    if open_us is not None and close_us is not None:
                        if close_us < open_us:
                            record_issue(
                                "close_before_open",
                                f"{csv_path.name}:{file_row_idx} open_us={open_us}, close_us={close_us}",
                            )
                        if expected_interval_us is not None:
                            open_resolution = timestamp_unit_resolution_us(open_unit)
                            close_resolution = timestamp_unit_resolution_us(close_unit)
                            candidate_resolutions = [
                                resolution
                                for resolution in (open_resolution, close_resolution)
                                if resolution is not None
                            ]
                            resolution_us = max(candidate_resolutions) if candidate_resolutions else 1
                            expected_close = open_us + expected_interval_us - resolution_us
                            if close_us != expected_close:
                                record_issue(
                                    "close_time_mismatch",
                                    f"{csv_path.name}:{file_row_idx} close_us={close_us}, "
                                    f"expected_close={expected_close}, resolution_us={resolution_us}",
                                )

                    try:
                        open_price = float(row_map["Open"])
                        high_price = float(row_map["High"])
                        low_price = float(row_map["Low"])
                        close_price = float(row_map["Close"])

                        if high_price < low_price:
                            record_issue(
                                "ohlc_high_below_low",
                                f"{csv_path.name}:{file_row_idx} high={high_price}, low={low_price}",
                            )
                        if high_price < max(open_price, close_price, low_price):
                            record_issue(
                                "ohlc_high_inconsistent",
                                f"{csv_path.name}:{file_row_idx} open={open_price}, high={high_price}, "
                                f"low={low_price}, close={close_price}",
                            )
                        if low_price > min(open_price, close_price, high_price):
                            record_issue(
                                "ohlc_low_inconsistent",
                                f"{csv_path.name}:{file_row_idx} open={open_price}, high={high_price}, "
                                f"low={low_price}, close={close_price}",
                            )
                    except ValueError:
                        record_issue(
                            "ohlc_parse_error",
                            f"{csv_path.name}:{file_row_idx} open/high/low/close parse failed",
                        )

                    try:
                        if float(row_map["Volume"]) < 0:
                            record_issue("negative_volume", f"{csv_path.name}:{file_row_idx}")
                    except ValueError:
                        record_issue("volume_parse_error", f"{csv_path.name}:{file_row_idx}")

                    try:
                        if int(row_map["Number of trades"]) < 0:
                            record_issue("negative_trade_count", f"{csv_path.name}:{file_row_idx}")
                    except ValueError:
                        record_issue("trades_parse_error", f"{csv_path.name}:{file_row_idx}")

                    if output_format == "csv":
                        csv_writer.writerow([row_map[col] for col in selected_columns])
                        rows_written += 1
                    else:
                        for col in selected_columns:
                            parquet_buffer[col].append(coerce_for_parquet(col, row_map[col]))
                        buffer_size += 1
                        rows_written += 1
                        if buffer_size >= config.parquet_chunk_size:
                            table = pa.Table.from_pydict(parquet_buffer, schema=parquet_schema)
                            parquet_writer.write_table(table)
                            parquet_buffer = {col: [] for col in selected_columns}
                            buffer_size = 0

        if output_format == "parquet" and buffer_size > 0:
            table = pa.Table.from_pydict(parquet_buffer, schema=parquet_schema)
            parquet_writer.write_table(table)
    finally:
        if csv_handle is not None:
            csv_handle.close()
        if parquet_writer is not None:
            parquet_writer.close()

    summary_lines = _build_summary_lines(
        config=config,
        extracted_dir=extracted_dir,
        combined_output_path=combined_output_path,
        summary_log_path=summary_log_path,
        selected_columns=selected_columns,
        selected_csvs=selected_csvs,
        files_processed=files_processed,
        rows_total=rows_total,
        rows_written=rows_written,
        issue_counts=issue_counts,
        issue_examples=dict(issue_examples),
        timestamp_units=timestamp_units,
        expected_interval_us=expected_interval_us,
    )
    summary_log_path.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")

    return CombineResult(
        project_root=project_root,
        extracted_dir=extracted_dir,
        output_dir=output_dir,
        combined_output_path=combined_output_path,
        summary_log_path=summary_log_path,
        selected_columns=selected_columns,
        selected_csvs=[item[3] for item in selected_csvs],
        files_processed=files_processed,
        rows_read=rows_total,
        rows_written=rows_written,
        issue_counts=dict(issue_counts),
        issue_examples=dict(issue_examples),
        timestamp_units_observed=dict(timestamp_units),
        expected_interval_us=expected_interval_us,
    )
