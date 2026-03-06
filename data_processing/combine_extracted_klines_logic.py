"""Logic for combining extracted Binance kline CSV files with integrity checks."""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
import csv
import itertools
import re

import pandas as pd

from data_processing.kline_constants import KLINE_COLUMNS
from data_processing.process_klines_monthly_logic import resolve_project_root


INT_COLUMNS = {"Open time", "Close time", "Number of trades", "Ignore"}
FLOAT_COLUMNS = {
    "Open",
    "High",
    "Low",
    "Close",
    "Volume",
    "Quote asset volume",
    "Taker buy base asset volume",
    "Taker buy quote asset volume",
}
TIMESTAMP_COLUMNS = {"Open time", "Close time"}
FLOAT_NAN_LITERALS = {"nan", "+nan", "-nan"}
TIMESTAMP_RESOLUTION_US = {"us": 1, "ms": 1000, "s": 1_000_000}


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
    column_renames: dict[str, str] = field(default_factory=dict)
    parquet_compression: str = "snappy"
    parquet_chunk_size: int = 200_000
    read_chunk_size: int = 200_000
    datetime_columns_to_utc: list[str] = field(default_factory=list)
    max_examples_per_issue: int = 15


@dataclass
class CombineResult:
    project_root: Path
    extracted_dir: Path
    output_dir: Path
    combined_output_path: Path
    summary_log_path: Path
    selected_columns: list[str]
    selected_source_columns: list[str]
    column_renames_applied: dict[str, str]
    datetime_columns_converted: list[str]
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
    return TIMESTAMP_RESOLUTION_US.get(unit) if unit is not None else None


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


def resolve_renamed_columns(
    selected_columns: list[str],
    column_renames: dict[str, str],
) -> tuple[list[str], dict[str, str]]:
    renames = dict(column_renames)

    unknown_sources = [col for col in renames if col not in KLINE_COLUMNS]
    if unknown_sources:
        raise ValueError(f"Unknown COLUMN_RENAMES source columns: {unknown_sources}")

    not_selected = [col for col in renames if col not in selected_columns]
    if not_selected:
        raise ValueError(
            "COLUMN_RENAMES can only rename selected columns. "
            f"Not selected: {not_selected}"
        )

    invalid_targets = [
        source_col
        for source_col, target_col in renames.items()
        if not isinstance(target_col, str) or not target_col.strip()
    ]
    if invalid_targets:
        raise ValueError(
            "COLUMN_RENAMES targets must be non-empty strings. "
            f"Invalid targets for: {invalid_targets}"
        )

    output_columns = [renames.get(col, col) for col in selected_columns]
    if len(output_columns) != len(set(output_columns)):
        duplicates = sorted({col for col in output_columns if output_columns.count(col) > 1})
        raise ValueError(f"COLUMN_RENAMES produced duplicate output columns: {duplicates}")

    applied = {col: renames[col] for col in selected_columns if col in renames}
    return output_columns, applied


def resolve_datetime_columns(
    selected_source_columns: list[str],
    output_columns: list[str],
    datetime_columns_to_utc: list[str],
) -> list[str]:
    converted = [col for col in datetime_columns_to_utc]
    if len(converted) != len(set(converted)):
        raise ValueError("datetime_columns_to_utc contains duplicates.")

    unknown = [col for col in converted if col not in output_columns]
    if unknown:
        raise ValueError(
            "datetime_columns_to_utc can only reference output columns. "
            f"Unknown columns: {unknown}"
        )

    invalid = [
        output_col
        for source_col, output_col in zip(selected_source_columns, output_columns)
        if output_col in converted and source_col not in TIMESTAMP_COLUMNS
    ]
    if invalid:
        raise ValueError(
            "datetime_columns_to_utc can only be used with Open time or Close time columns. "
            f"Invalid columns: {invalid}"
        )

    return converted


def default_extracted_dir(project_root: Path) -> Path:
    return project_root / "downloads" / "test1"


def resolve_override_path(project_root: Path, override: Path | None) -> Path | None:
    if override is None:
        return None
    if override.is_absolute():
        return override
    return project_root / override


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


def parse_integral_series(raw: pd.Series) -> tuple[pd.Series, pd.Series]:
    normalized = raw.str.strip()
    valid_mask = normalized.str.fullmatch(r"[+-]?\d+")
    parsed = pd.Series(pd.NA, index=raw.index, dtype="Int64")
    if valid_mask.any():
        parsed.loc[valid_mask] = normalized.loc[valid_mask].astype("int64")
    parse_error_mask = ~valid_mask
    return parsed, parse_error_mask


def normalize_timestamp_series(raw: pd.Series) -> tuple[pd.Series, pd.Series, pd.Series]:
    parsed, parse_error_mask = parse_integral_series(raw)
    normalized = pd.Series(pd.NA, index=raw.index, dtype="Int64")
    units = pd.Series([None] * len(raw), index=raw.index, dtype="object")

    valid_mask = ~parse_error_mask
    if valid_mask.any():
        values = parsed.loc[valid_mask].astype("int64")

        us_index = values.index[values >= 10**15]
        ms_index = values.index[(values >= 10**12) & (values < 10**15)]
        s_index = values.index[(values >= 10**9) & (values < 10**12)]
        unknown_index = values.index[values < 10**9]

        if len(us_index) > 0:
            normalized.loc[us_index] = values.loc[us_index].to_numpy()
            units.loc[us_index] = "us"
        if len(ms_index) > 0:
            normalized.loc[ms_index] = (values.loc[ms_index] * 1000).to_numpy()
            units.loc[ms_index] = "ms"
        if len(s_index) > 0:
            normalized.loc[s_index] = (values.loc[s_index] * 1_000_000).to_numpy()
            units.loc[s_index] = "s"
        if len(unknown_index) > 0:
            units.loc[unknown_index] = "unknown"

    return normalized, units, parse_error_mask


def parse_float_series(raw: pd.Series) -> tuple[pd.Series, pd.Series]:
    normalized = raw.str.strip()
    numeric = pd.to_numeric(normalized, errors="coerce")
    nan_literals = normalized.str.lower().isin(FLOAT_NAN_LITERALS)
    parse_error_mask = numeric.isna() & ~nan_literals
    return numeric, parse_error_mask


def record_issue_matches(
    issue_counts: Counter,
    issue_examples: defaultdict[str, list[str]],
    issue_key: str,
    match_mask: pd.Series,
    detail_builder: Callable[[int], str],
    max_examples_per_issue: int,
) -> None:
    normalized_mask = match_mask.fillna(False)
    match_index = normalized_mask[normalized_mask].index
    count = len(match_index)
    if count == 0:
        return

    issue_counts[issue_key] += count
    remaining = max_examples_per_issue - len(issue_examples[issue_key])
    if remaining <= 0:
        return

    for idx in match_index[:remaining]:
        issue_examples[issue_key].append(detail_builder(int(idx)))


def build_parquet_schema(
    pa,
    selected_source_columns: list[str],
    output_columns: list[str],
    datetime_columns_to_utc: list[str],
):
    parquet_field_types = []
    datetime_columns = set(datetime_columns_to_utc)
    for source_col, output_col in zip(selected_source_columns, output_columns):
        if output_col in datetime_columns:
            parquet_field_types.append((output_col, pa.timestamp("us", tz="UTC")))
        elif source_col in INT_COLUMNS:
            parquet_field_types.append((output_col, pa.int64()))
        elif source_col in FLOAT_COLUMNS:
            parquet_field_types.append((output_col, pa.float64()))
        else:
            parquet_field_types.append((output_col, pa.string()))
    return pa.schema(parquet_field_types)


def coerce_output_chunk_for_parquet(
    output_chunk: pd.DataFrame,
    selected_source_columns: list[str],
    output_columns: list[str],
    datetime_columns_to_utc: list[str],
) -> pd.DataFrame:
    parquet_chunk = output_chunk.copy()
    datetime_columns = set(datetime_columns_to_utc)

    for source_col, output_col in zip(selected_source_columns, output_columns):
        if output_col in datetime_columns:
            continue
        if source_col in INT_COLUMNS:
            parsed, _ = parse_integral_series(parquet_chunk[output_col].astype(str))
            parquet_chunk[output_col] = parsed
        elif source_col in FLOAT_COLUMNS:
            parsed, _ = parse_float_series(parquet_chunk[output_col].astype(str))
            parquet_chunk[output_col] = parsed
        else:
            parquet_chunk[output_col] = parquet_chunk[output_col].astype("string")

    return parquet_chunk


def _build_summary_lines(
    config: CombineConfig,
    extracted_dir: Path,
    combined_output_path: Path,
    summary_log_path: Path,
    selected_source_columns: list[str],
    output_columns: list[str],
    column_renames_applied: dict[str, str],
    datetime_columns_converted: list[str],
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
        f"READ_CHUNK_SIZE={config.read_chunk_size}",
        f"PARQUET_CHUNK_SIZE={config.parquet_chunk_size}",
        f"SELECTED_SOURCE_COLUMNS={selected_source_columns}",
        f"OUTPUT_COLUMNS={output_columns}",
        f"COLUMN_RENAMES_APPLIED={column_renames_applied}",
        f"DATETIME_COLUMNS_TO_UTC={datetime_columns_converted}",
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
    if config.read_chunk_size <= 0:
        raise ValueError("read_chunk_size must be > 0.")
    if config.parquet_chunk_size <= 0:
        raise ValueError("parquet_chunk_size must be > 0.")

    project_root = resolve_project_root(config.project_root)
    extracted_dir_override = resolve_override_path(project_root, config.extracted_dir)
    output_dir_override = resolve_override_path(project_root, config.output_dir)
    extracted_dir = extracted_dir_override or default_extracted_dir(project_root)
    output_dir = output_dir_override or extracted_dir

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

    selected_source_columns = resolve_output_columns(KLINE_COLUMNS, config.keep_columns, config.drop_columns)
    output_columns, column_renames_applied = resolve_renamed_columns(
        selected_columns=selected_source_columns,
        column_renames=config.column_renames,
    )
    datetime_columns_converted = resolve_datetime_columns(
        selected_source_columns=selected_source_columns,
        output_columns=output_columns,
        datetime_columns_to_utc=config.datetime_columns_to_utc,
    )
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

    def iter_valid_row_chunks(csv_path: Path):
        nonlocal rows_total

        with csv_path.open("r", newline="", encoding="utf-8") as src:
            reader = csv.reader(src)
            first_row = next(reader, None)
            if first_row is None:
                record_issue("empty_file", f"{csv_path.name}: file has no rows")
                return

            file_row_idx = 0
            row_buffer: list[list[str]] = []
            row_number_buffer: list[int] = []

            def flush_buffer():
                nonlocal row_buffer, row_number_buffer
                if not row_buffer:
                    return None

                chunk = pd.DataFrame(row_buffer, columns=KLINE_COLUMNS)
                file_row_numbers = pd.Series(row_number_buffer, index=chunk.index, dtype="int64")
                row_buffer = []
                row_number_buffer = []
                return chunk, file_row_numbers

            if first_row == KLINE_COLUMNS:
                row_source = reader
            else:
                record_issue(
                    "missing_or_unexpected_header",
                    f"{csv_path.name}: first row is not expected header",
                )
                row_source = itertools.chain([first_row], reader)

            for row in row_source:
                file_row_idx += 1
                rows_total += 1

                if len(row) != len(KLINE_COLUMNS):
                    record_issue(
                        "malformed_row_column_count",
                        f"{csv_path.name}:{file_row_idx} has {len(row)} columns (expected {len(KLINE_COLUMNS)})",
                    )
                    continue

                row_buffer.append(row)
                row_number_buffer.append(file_row_idx)

                if len(row_buffer) >= config.read_chunk_size:
                    flushed = flush_buffer()
                    if flushed is not None:
                        yield flushed

            flushed = flush_buffer()
            if flushed is not None:
                yield flushed

    if output_format == "csv":
        pd.DataFrame(columns=output_columns).to_csv(
            combined_output_path,
            index=False,
            lineterminator="\n",
        )
        parquet_writer = None
        parquet_schema = None
        wrote_parquet_rows = False
    else:
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError as exc:
            raise ImportError("Parquet output requires pyarrow. Install it, then rerun.") from exc

        parquet_schema = build_parquet_schema(
            pa=pa,
            selected_source_columns=selected_source_columns,
            output_columns=output_columns,
            datetime_columns_to_utc=datetime_columns_converted,
        )
        parquet_writer = pq.ParquetWriter(
            str(combined_output_path),
            parquet_schema,
            compression=config.parquet_compression,
        )
        wrote_parquet_rows = False

    prev_open_time_us: int | None = None

    try:
        for _, _, _, csv_path in selected_csvs:
            files_processed += 1

            for chunk_df, file_row_numbers in iter_valid_row_chunks(csv_path):
                file_name = csv_path.name

                open_raw = chunk_df["Open time"]
                close_raw = chunk_df["Close time"]

                open_us, open_units, open_parse_error = normalize_timestamp_series(open_raw)
                close_us, close_units, close_parse_error = normalize_timestamp_series(close_raw)

                timestamp_units.update(unit for unit in open_units.dropna().tolist())
                timestamp_units.update(unit for unit in close_units.dropna().tolist())

                def row_ref(idx: int) -> str:
                    return f"{file_name}:{int(file_row_numbers.loc[idx])}"

                record_issue_matches(
                    issue_counts,
                    issue_examples,
                    "open_time_parse_error",
                    open_parse_error,
                    lambda idx: f"{row_ref(idx)} open_time={open_raw.loc[idx]}",
                    config.max_examples_per_issue,
                )
                record_issue_matches(
                    issue_counts,
                    issue_examples,
                    "close_time_parse_error",
                    close_parse_error,
                    lambda idx: f"{row_ref(idx)} close_time={close_raw.loc[idx]}",
                    config.max_examples_per_issue,
                )

                open_unknown_mask = open_units == "unknown"
                close_unknown_mask = close_units == "unknown"
                record_issue_matches(
                    issue_counts,
                    issue_examples,
                    "open_time_unknown_unit",
                    open_unknown_mask,
                    lambda idx: f"{row_ref(idx)} open_time={open_raw.loc[idx]}",
                    config.max_examples_per_issue,
                )
                record_issue_matches(
                    issue_counts,
                    issue_examples,
                    "close_time_unknown_unit",
                    close_unknown_mask,
                    lambda idx: f"{row_ref(idx)} close_time={close_raw.loc[idx]}",
                    config.max_examples_per_issue,
                )

                valid_open_mask = open_us.notna()
                if valid_open_mask.any():
                    valid_open_us = open_us.loc[valid_open_mask].astype("int64")
                    prev_values = valid_open_us.shift(1)
                    if prev_open_time_us is not None:
                        prev_values.iloc[0] = prev_open_time_us

                    comparable = prev_values.notna()
                    deltas = valid_open_us - prev_values

                    timestamp_decrease_mask = comparable & (deltas < 0)
                    timestamp_duplicate_mask = comparable & (deltas == 0)

                    record_issue_matches(
                        issue_counts,
                        issue_examples,
                        "timestamp_decrease",
                        timestamp_decrease_mask,
                        lambda idx: f"{row_ref(idx)} delta_us={int(deltas.loc[idx])}",
                        config.max_examples_per_issue,
                    )
                    record_issue_matches(
                        issue_counts,
                        issue_examples,
                        "timestamp_duplicate",
                        timestamp_duplicate_mask,
                        lambda idx: f"{row_ref(idx)} duplicate open time",
                        config.max_examples_per_issue,
                    )

                    if expected_interval_us is not None:
                        positive_delta_mask = comparable & (deltas > 0)
                        timestamp_step_mismatch_mask = positive_delta_mask & (deltas != expected_interval_us)
                        timestamp_gap_mask = timestamp_step_mismatch_mask & (deltas > expected_interval_us)
                        timestamp_overlap_mask = timestamp_step_mismatch_mask & (deltas < expected_interval_us)

                        record_issue_matches(
                            issue_counts,
                            issue_examples,
                            "timestamp_step_mismatch",
                            timestamp_step_mismatch_mask,
                            lambda idx: (
                                f"{row_ref(idx)} delta_us={int(deltas.loc[idx])}, "
                                f"expected={expected_interval_us}"
                            ),
                            config.max_examples_per_issue,
                        )
                        record_issue_matches(
                            issue_counts,
                            issue_examples,
                            "timestamp_gap",
                            timestamp_gap_mask,
                            lambda idx: (
                                f"{row_ref(idx)} gap_us={int(deltas.loc[idx]) - expected_interval_us}"
                            ),
                            config.max_examples_per_issue,
                        )
                        record_issue_matches(
                            issue_counts,
                            issue_examples,
                            "timestamp_overlap",
                            timestamp_overlap_mask,
                            lambda idx: (
                                f"{row_ref(idx)} overlap_us={expected_interval_us - int(deltas.loc[idx])}"
                            ),
                            config.max_examples_per_issue,
                        )

                    prev_open_time_us = int(valid_open_us.iloc[-1])

                open_close_unit_mismatch_mask = open_units.notna() & close_units.notna() & (open_units != close_units)
                record_issue_matches(
                    issue_counts,
                    issue_examples,
                    "open_close_time_unit_mismatch",
                    open_close_unit_mismatch_mask,
                    lambda idx: (
                        f"{row_ref(idx)} open_unit={open_units.loc[idx]}, close_unit={close_units.loc[idx]}"
                    ),
                    config.max_examples_per_issue,
                )

                both_valid_mask = open_us.notna() & close_us.notna()
                if both_valid_mask.any():
                    valid_pair_index = both_valid_mask[both_valid_mask].index
                    open_us_valid = open_us.loc[valid_pair_index].astype("int64")
                    close_us_valid = close_us.loc[valid_pair_index].astype("int64")

                    close_before_open_mask = close_us_valid < open_us_valid
                    record_issue_matches(
                        issue_counts,
                        issue_examples,
                        "close_before_open",
                        close_before_open_mask,
                        lambda idx: (
                            f"{row_ref(idx)} open_us={int(open_us_valid.loc[idx])}, "
                            f"close_us={int(close_us_valid.loc[idx])}"
                        ),
                        config.max_examples_per_issue,
                    )

                    if expected_interval_us is not None:
                        open_resolution = open_units.map(TIMESTAMP_RESOLUTION_US).loc[valid_pair_index].astype("int64")
                        close_resolution = close_units.map(TIMESTAMP_RESOLUTION_US).loc[valid_pair_index].astype("int64")
                        resolution_us = pd.concat([open_resolution, close_resolution], axis=1).max(axis=1)
                        expected_close = open_us_valid + expected_interval_us - resolution_us
                        close_time_mismatch_mask = close_us_valid != expected_close

                        record_issue_matches(
                            issue_counts,
                            issue_examples,
                            "close_time_mismatch",
                            close_time_mismatch_mask,
                            lambda idx: (
                                f"{row_ref(idx)} close_us={int(close_us_valid.loc[idx])}, "
                                f"expected_close={int(expected_close.loc[idx])}, "
                                f"resolution_us={int(resolution_us.loc[idx])}"
                            ),
                            config.max_examples_per_issue,
                        )

                open_price, open_price_parse_error = parse_float_series(chunk_df["Open"])
                high_price, high_price_parse_error = parse_float_series(chunk_df["High"])
                low_price, low_price_parse_error = parse_float_series(chunk_df["Low"])
                close_price, close_price_parse_error = parse_float_series(chunk_df["Close"])

                ohlc_parse_error_mask = (
                    open_price_parse_error
                    | high_price_parse_error
                    | low_price_parse_error
                    | close_price_parse_error
                )
                record_issue_matches(
                    issue_counts,
                    issue_examples,
                    "ohlc_parse_error",
                    ohlc_parse_error_mask,
                    lambda idx: f"{row_ref(idx)} open/high/low/close parse failed",
                    config.max_examples_per_issue,
                )

                ohlc_valid_mask = ~ohlc_parse_error_mask
                record_issue_matches(
                    issue_counts,
                    issue_examples,
                    "ohlc_high_below_low",
                    ohlc_valid_mask & (high_price < low_price),
                    lambda idx: f"{row_ref(idx)} high={high_price.loc[idx]}, low={low_price.loc[idx]}",
                    config.max_examples_per_issue,
                )
                record_issue_matches(
                    issue_counts,
                    issue_examples,
                    "ohlc_high_inconsistent",
                    ohlc_valid_mask & (high_price < pd.concat([open_price, close_price, low_price], axis=1).max(axis=1)),
                    lambda idx: (
                        f"{row_ref(idx)} open={open_price.loc[idx]}, high={high_price.loc[idx]}, "
                        f"low={low_price.loc[idx]}, close={close_price.loc[idx]}"
                    ),
                    config.max_examples_per_issue,
                )
                record_issue_matches(
                    issue_counts,
                    issue_examples,
                    "ohlc_low_inconsistent",
                    ohlc_valid_mask & (low_price > pd.concat([open_price, close_price, high_price], axis=1).min(axis=1)),
                    lambda idx: (
                        f"{row_ref(idx)} open={open_price.loc[idx]}, high={high_price.loc[idx]}, "
                        f"low={low_price.loc[idx]}, close={close_price.loc[idx]}"
                    ),
                    config.max_examples_per_issue,
                )

                volume_values, volume_parse_error = parse_float_series(chunk_df["Volume"])
                record_issue_matches(
                    issue_counts,
                    issue_examples,
                    "volume_parse_error",
                    volume_parse_error,
                    lambda idx: row_ref(idx),
                    config.max_examples_per_issue,
                )
                record_issue_matches(
                    issue_counts,
                    issue_examples,
                    "negative_volume",
                    (~volume_parse_error) & (volume_values < 0),
                    lambda idx: row_ref(idx),
                    config.max_examples_per_issue,
                )

                trade_values, trades_parse_error = parse_integral_series(chunk_df["Number of trades"])
                record_issue_matches(
                    issue_counts,
                    issue_examples,
                    "trades_parse_error",
                    trades_parse_error,
                    lambda idx: row_ref(idx),
                    config.max_examples_per_issue,
                )
                record_issue_matches(
                    issue_counts,
                    issue_examples,
                    "negative_trade_count",
                    (~trades_parse_error) & (trade_values < 0),
                    lambda idx: row_ref(idx),
                    config.max_examples_per_issue,
                )

                output_chunk = chunk_df.loc[:, selected_source_columns].copy()
                output_chunk.columns = output_columns

                for source_col, output_col in zip(selected_source_columns, output_columns):
                    if output_col not in datetime_columns_converted:
                        continue
                    source_us = open_us if source_col == "Open time" else close_us
                    output_chunk[output_col] = pd.to_datetime(
                        source_us.astype("float64"),
                        unit="us",
                        utc=True,
                        errors="coerce",
                    )

                if output_format == "csv":
                    output_chunk.to_csv(
                        combined_output_path,
                        mode="a",
                        index=False,
                        header=False,
                        lineterminator="\n",
                    )
                else:
                    parquet_chunk = coerce_output_chunk_for_parquet(
                        output_chunk=output_chunk,
                        selected_source_columns=selected_source_columns,
                        output_columns=output_columns,
                        datetime_columns_to_utc=datetime_columns_converted,
                    )
                    table = pa.Table.from_pandas(
                        parquet_chunk,
                        schema=parquet_schema,
                        preserve_index=False,
                    )
                    parquet_writer.write_table(table, row_group_size=config.parquet_chunk_size)
                    wrote_parquet_rows = True

                rows_written += len(output_chunk)
    finally:
        if parquet_writer is not None:
            if not wrote_parquet_rows:
                empty_table = pa.Table.from_arrays(
                    [pa.array([], type=field.type) for field in parquet_schema],
                    schema=parquet_schema,
                )
                parquet_writer.write_table(empty_table, row_group_size=config.parquet_chunk_size)
            parquet_writer.close()

    summary_lines = _build_summary_lines(
        config=config,
        extracted_dir=extracted_dir,
        combined_output_path=combined_output_path,
        summary_log_path=summary_log_path,
        selected_source_columns=selected_source_columns,
        output_columns=output_columns,
        column_renames_applied=column_renames_applied,
        datetime_columns_converted=datetime_columns_converted,
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
        selected_columns=output_columns,
        selected_source_columns=selected_source_columns,
        column_renames_applied=column_renames_applied,
        datetime_columns_converted=datetime_columns_converted,
        selected_csvs=[item[3] for item in selected_csvs],
        files_processed=files_processed,
        rows_read=rows_total,
        rows_written=rows_written,
        issue_counts=dict(issue_counts),
        issue_examples=dict(issue_examples),
        timestamp_units_observed=dict(timestamp_units),
        expected_interval_us=expected_interval_us,
    )
