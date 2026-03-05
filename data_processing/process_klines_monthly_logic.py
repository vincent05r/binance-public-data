"""Logic for extracting Binance monthly spot kline ZIP files into CSV files."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import csv
import io
import re
from zipfile import ZipFile

from data_processing.kline_constants import KLINE_COLUMNS


@dataclass
class ExtractConfig:
    symbol: str = "BTCUSDT"
    interval: str = "1s"
    project_root: Path | None = None
    source_dir: Path | None = None
    output_dir: Path | None = None
    overwrite_existing: bool = True


@dataclass
class ExtractResult:
    project_root: Path
    source_dir: Path
    output_dir: Path
    selected_zips: list[Path]
    extracted_csv_paths: list[Path]


def resolve_project_root(base_path: Path | None = None) -> Path:
    """Resolve repository root based on presence of the downloads directory."""
    project_root = base_path or Path.cwd()
    if not (project_root / "downloads").exists() and (project_root.parent / "downloads").exists():
        project_root = project_root.parent
    return project_root


def build_source_dir(project_root: Path, symbol: str, interval: str) -> Path:
    return project_root / "downloads" / "data" / "spot" / "monthly" / "klines" / symbol / interval


def build_output_dir(project_root: Path) -> Path:
    return project_root / "downloads" / "test1"


def select_zip_files(source_dir: Path, symbol: str, interval: str) -> list[tuple[datetime, str, Path]]:
    zip_pattern = re.compile(
        rf"^{re.escape(symbol)}-{re.escape(interval)}-(\d{{4}}-\d{{2}})\.zip$",
        flags=re.IGNORECASE,
    )

    selected = []
    for zip_path in source_dir.iterdir():
        if not zip_path.is_file() or zip_path.suffix.lower() != ".zip":
            continue
        match = zip_pattern.match(zip_path.name)
        if not match:
            continue
        month_text = match.group(1)
        month_dt = datetime.strptime(month_text, "%Y-%m")
        selected.append((month_dt, month_text, zip_path))

    selected.sort(key=lambda item: item[0])
    return selected


def _write_csv_with_header(
    zf: ZipFile,
    member_name: str,
    output_csv_path: Path,
) -> None:
    with zf.open(member_name, "r") as src_bin, output_csv_path.open(
        "w",
        newline="",
        encoding="utf-8",
    ) as dst:
        reader = csv.reader(io.TextIOWrapper(src_bin, encoding="utf-8"))
        writer = csv.writer(dst)
        writer.writerow(KLINE_COLUMNS)

        first_row = next(reader, None)
        if first_row is not None:
            # Most Binance files do not include a header. If one exists, do not duplicate it.
            if first_row != KLINE_COLUMNS:
                writer.writerow(first_row)
            for row in reader:
                writer.writerow(row)


def extract_monthly_klines(config: ExtractConfig) -> ExtractResult:
    """Extract all monthly ZIP files in source_dir and restore CSV headers."""
    project_root = resolve_project_root(config.project_root)
    source_dir = config.source_dir or build_source_dir(project_root, config.symbol, config.interval)
    output_dir = config.output_dir or build_output_dir(project_root)

    if not source_dir.exists():
        raise FileNotFoundError(f"Source directory not found: {source_dir}")

    selected = select_zip_files(source_dir, config.symbol, config.interval)
    if not selected:
        raise ValueError(f"No ZIP files found in expected format under {source_dir}")

    output_dir.mkdir(parents=True, exist_ok=True)
    extracted_csv_paths: list[Path] = []

    for _, month_text, zip_path in selected:
        with ZipFile(zip_path, "r") as zf:
            csv_members = sorted(name for name in zf.namelist() if name.lower().endswith(".csv"))
            if not csv_members:
                raise ValueError(f"No CSV file found in ZIP: {zip_path}")

            for idx, member in enumerate(csv_members, start=1):
                suffix = "" if len(csv_members) == 1 else f"-part{idx}"
                output_name = f"{config.symbol}-{config.interval}-{month_text}{suffix}.csv"
                output_csv_path = output_dir / output_name

                if output_csv_path.exists() and not config.overwrite_existing:
                    extracted_csv_paths.append(output_csv_path)
                    continue

                _write_csv_with_header(zf, member, output_csv_path)
                extracted_csv_paths.append(output_csv_path)

    return ExtractResult(
        project_root=project_root,
        source_dir=source_dir,
        output_dir=output_dir,
        selected_zips=[item[2] for item in selected],
        extracted_csv_paths=extracted_csv_paths,
    )

