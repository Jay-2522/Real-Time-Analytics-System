"""Chunked CIC-IDS-2017 flow loading and cleaning.

Dataset citation: I. Sharafaldin, A. H. Lashkari, and A. A. Ghorbani,
"Toward Generating a New Intrusion Detection Dataset and Intrusion Traffic
Characterization," ICISSP 2018.

The distributed CSVs use a 12-hour workday clock without AM/PM. Values before
08:00 belong to the afternoon session and are shifted by 12 hours. Monday has
second-resolution timestamps; the other supplied files have minute resolution.
The second duplicate ``Fwd Header Length`` column is deliberately ignored.
"""

from __future__ import annotations

import csv
import os
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Sequence

import numpy as np
import pandas as pd


DEFAULT_CICIDS_DIR = Path.home() / "datasets/cicids2017/TrafficLabelling"
FILE_GROUPS: dict[str, tuple[str, ...]] = {
    "Monday": ("Monday-WorkingHours.pcap_ISCX.csv",),
    "Tuesday": ("Tuesday-WorkingHours.pcap_ISCX.csv",),
    "Wednesday": ("Wednesday-workingHours.pcap_ISCX.csv",),
    "Thursday": (
        "Thursday-WorkingHours-Morning-WebAttacks.pcap_ISCX.csv",
        "Thursday-WorkingHours-Afternoon-Infilteration.pcap_ISCX.csv",
    ),
    "Friday": (
        "Friday-WorkingHours-Morning.pcap_ISCX.csv",
        "Friday-WorkingHours-Afternoon-PortScan.pcap_ISCX.csv",
        "Friday-WorkingHours-Afternoon-DDos.pcap_ISCX.csv",
    ),
}

IDENTITY_COLUMNS = {
    "flow_id",
    "source_ip",
    "source_port",
    "destination_ip",
    "raw_timestamp",
    "timestamp",
}
NUMERIC_COLUMNS = (
    "source_port",
    "destination_port",
    "flow_duration",
    "total_fwd_packets",
    "total_backward_packets",
    "total_length_of_fwd_packets",
    "total_length_of_bwd_packets",
    "flow_bytes_per_s",
    "syn_flag_count",
    "rst_flag_count",
    "psh_flag_count",
    "ack_flag_count",
    "average_packet_size",
    "fwd_header_length",
)
SELECTED_COLUMNS = {
    "flow_id",
    "source_ip",
    "source_port",
    "destination_ip",
    "destination_port",
    "timestamp",
    "flow_duration",
    "total_fwd_packets",
    "total_backward_packets",
    "total_length_of_fwd_packets",
    "total_length_of_bwd_packets",
    "flow_bytes_per_s",
    "syn_flag_count",
    "rst_flag_count",
    "psh_flag_count",
    "ack_flag_count",
    "average_packet_size",
    "fwd_header_length",
    "label",
}


@dataclass(frozen=True)
class FileProfile:
    name: str
    rows: int
    blank_rows_dropped: int
    benign: int
    attack: int
    labels: dict[str, int]
    attack_sources: dict[str, dict[str, int]]
    infinity_values: int
    nan_values: int
    raw_min_timestamp: str
    raw_max_timestamp: str
    corrected_min_timestamp: str
    corrected_max_timestamp: str
    timestamp_resolution: str
    leading_space_columns: int
    duplicate_columns: tuple[str, ...]


def dataset_root() -> Path:
    return Path(os.environ.get("CICIDS_DIR", DEFAULT_CICIDS_DIR)).expanduser()


def normalize_column(name: str) -> str:
    stripped = name.strip()
    stripped = stripped.replace("/s", "_per_s")
    return re.sub(r"[^a-z0-9]+", "_", stripped.casefold()).strip("_")


def raw_header(path: Path) -> list[str]:
    with path.open("r", encoding="cp1252", newline="") as handle:
        return next(csv.reader(handle))


def header_findings(path: Path) -> tuple[int, tuple[str, ...]]:
    header = raw_header(path)
    normalized = [normalize_column(column) for column in header]
    duplicates = tuple(sorted(name for name, count in Counter(normalized).items() if count > 1))
    return sum(column != column.strip() for column in header), duplicates


def paths_for_day(day: str, root: Path | None = None) -> tuple[Path, ...]:
    base = root or dataset_root()
    try:
        names = FILE_GROUPS[day]
    except KeyError as exc:
        raise ValueError(f"Unknown CIC-IDS-2017 day: {day}") from exc
    paths = tuple(base / name for name in names)
    missing = [str(path) for path in paths if not path.is_file()]
    if missing:
        raise FileNotFoundError(f"Missing CIC-IDS-2017 files: {missing}")
    return paths


def _read_raw_chunks(
    path: Path,
    chunksize: int,
    columns: set[str] | None = None,
) -> Iterator[pd.DataFrame]:
    wanted = columns or SELECTED_COLUMNS

    def use_column(raw_name: str) -> bool:
        canonical = normalize_column(raw_name)
        # pandas suffixes the second duplicate header with '.1'. Keep only the
        # first Fwd Header Length because the duplicate contains the same field.
        return canonical in wanted and not raw_name.strip().endswith(".1")

    yield from pd.read_csv(
        path,
        encoding="cp1252",
        chunksize=chunksize,
        low_memory=False,
        usecols=use_column,
    )


def clean_chunk(raw: pd.DataFrame) -> pd.DataFrame:
    frame = raw.rename(columns={column: normalize_column(column) for column in raw.columns}).copy()
    for column in ("flow_id", "source_ip", "destination_ip", "label"):
        if column in frame:
            frame[column] = frame[column].astype("string").str.strip()

    required = [column for column in ("source_ip", "timestamp", "label") if column in frame]
    if required:
        frame = frame.dropna(subset=required, how="all")
    if "label" in frame:
        frame = frame[frame["label"].notna() & frame["label"].ne("")].copy()

    if "timestamp" in frame:
        raw_timestamps = frame["timestamp"].astype("string").str.strip()
        parsed = pd.to_datetime(raw_timestamps, format="mixed", dayfirst=True, errors="coerce")
        if parsed.isna().any():
            raise ValueError(f"Unparseable timestamps found: {raw_timestamps[parsed.isna()].head(3).tolist()}")
        afternoon = parsed.dt.hour < 8
        frame["raw_timestamp"] = raw_timestamps
        frame["timestamp"] = parsed + pd.to_timedelta(afternoon.astype(int) * 12, unit="h")

    for column in NUMERIC_COLUMNS:
        if column in frame:
            frame[column] = pd.to_numeric(frame[column], errors="coerce").replace([np.inf, -np.inf], np.nan)

    if "label" in frame:
        frame["is_attack"] = frame["label"].ne("BENIGN")
    return frame.reset_index(drop=True)


def iter_clean_chunks(path: Path, chunksize: int = 100_000) -> Iterator[pd.DataFrame]:
    for raw in _read_raw_chunks(path, chunksize):
        cleaned = clean_chunk(raw)
        if not cleaned.empty:
            yield cleaned


def load_file(path: Path, chunksize: int = 100_000) -> pd.DataFrame:
    chunks = list(iter_clean_chunks(path, chunksize))
    if not chunks:
        raise ValueError(f"No usable flow rows in {path}")
    return pd.concat(chunks, ignore_index=True)


def profile_file(path: Path, chunksize: int = 100_000) -> FileProfile:
    labels: Counter[str] = Counter()
    attack_sources: dict[str, Counter[str]] = defaultdict(Counter)
    rows = blank_rows = infinity_values = nan_values = 0
    raw_min = raw_max = corrected_min = corrected_max = None
    has_seconds = False

    for raw in _read_raw_chunks(path, chunksize):
        raw_rows = len(raw)
        cleaned = clean_chunk(raw)
        rows += len(cleaned)
        blank_rows += raw_rows - len(cleaned)
        if "flow_bytes_per_s" in cleaned:
            canonical_raw = raw.rename(columns={column: normalize_column(column) for column in raw.columns})
            valid_rows = canonical_raw["label"].notna() & canonical_raw["label"].astype("string").str.strip().ne("")
            original = pd.to_numeric(canonical_raw.loc[valid_rows, "flow_bytes_per_s"], errors="coerce").to_numpy()
            infinity_values += int(np.isinf(original).sum())
            nan_values += int(np.isnan(original).sum())
        labels.update(cleaned["label"].tolist())
        attacks = cleaned[cleaned["is_attack"]]
        for label, group in attacks.groupby("label"):
            attack_sources[str(label)].update(group["source_ip"].astype(str).tolist())
        raw_values = cleaned["raw_timestamp"].astype(str)
        has_seconds = has_seconds or bool(raw_values.str.count(":").gt(1).any())
        raw_parsed = pd.to_datetime(raw_values, format="mixed", dayfirst=True)
        raw_min = raw_parsed.min() if raw_min is None else min(raw_min, raw_parsed.min())
        raw_max = raw_parsed.max() if raw_max is None else max(raw_max, raw_parsed.max())
        corrected_min = cleaned["timestamp"].min() if corrected_min is None else min(corrected_min, cleaned["timestamp"].min())
        corrected_max = cleaned["timestamp"].max() if corrected_max is None else max(corrected_max, cleaned["timestamp"].max())

    leading, duplicates = header_findings(path)
    benign = labels.get("BENIGN", 0)
    return FileProfile(
        name=path.name,
        rows=rows,
        blank_rows_dropped=blank_rows,
        benign=benign,
        attack=rows - benign,
        labels=dict(sorted(labels.items())),
        attack_sources={label: dict(counter.most_common()) for label, counter in sorted(attack_sources.items())},
        infinity_values=infinity_values,
        nan_values=nan_values,
        raw_min_timestamp=str(raw_min),
        raw_max_timestamp=str(raw_max),
        corrected_min_timestamp=str(corrected_min),
        corrected_max_timestamp=str(corrected_max),
        timestamp_resolution="seconds" if has_seconds else "minutes",
        leading_space_columns=leading,
        duplicate_columns=duplicates,
    )


def profile_dataset(root: Path | None = None) -> list[FileProfile]:
    base = root or dataset_root()
    expected = [base / name for names in FILE_GROUPS.values() for name in names]
    return [profile_file(path) for path in expected]


def print_profiles(profiles: Sequence[FileProfile]) -> None:
    print("file | rows | benign | attack | attack types | attack source IPs")
    for profile in profiles:
        attack_types = ", ".join(label for label in profile.labels if label != "BENIGN") or "none"
        sources = "; ".join(
            f"{label}: {', '.join(f'{ip} ({count})' for ip, count in counts.items())}"
            for label, counts in profile.attack_sources.items()
        ) or "none"
        print(f"{profile.name} | {profile.rows} | {profile.benign} | {profile.attack} | {attack_types} | {sources}")
    print("\ncleaning findings")
    for profile in profiles:
        print(
            f"{profile.name}: leading_headers={profile.leading_space_columns} "
            f"duplicates={profile.duplicate_columns} blank_rows={profile.blank_rows_dropped} "
            f"flow_bytes_per_s_inf={profile.infinity_values} flow_bytes_per_s_nan={profile.nan_values} "
            f"resolution={profile.timestamp_resolution} raw={profile.raw_min_timestamp}..{profile.raw_max_timestamp} "
            f"corrected={profile.corrected_min_timestamp}..{profile.corrected_max_timestamp} labels={profile.labels}"
        )


if __name__ == "__main__":
    print_profiles(profile_dataset())
