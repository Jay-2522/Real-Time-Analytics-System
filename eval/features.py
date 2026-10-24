"""Per-source-IP, one-minute CIC-IDS-2017 window features.

Most supplied timestamps have only minute resolution, so a 30-second slide
would create artificial duplicate placement. Each flow belongs to exactly one
calendar-minute window, matching the pipeline's source-IP/60-second identity.
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd

from eval.data import IDENTITY_COLUMNS, load_file, paths_for_day


FEATURE_COLUMNS = [
    "flow_count",
    "distinct_destination_ports",
    "distinct_destination_ips",
    "top_destination_port_flow_count",
    "top_destination_ip_flow_count",
    "total_fwd_packets",
    "total_bwd_packets",
    "total_fwd_bytes",
    "total_bwd_bytes",
    "mean_flow_duration",
    "syn_flag_sum",
    "rst_flag_sum",
    "ack_flag_sum",
    "psh_flag_sum",
    "mean_packet_size",
]


def assert_no_identity_leakage(columns: Iterable[str]) -> None:
    leaked = sorted(set(columns) & IDENTITY_COLUMNS)
    if leaked:
        raise ValueError(f"Identity or raw-time columns cannot reach a model: {leaked}")


def aggregate_windows(flows: pd.DataFrame) -> pd.DataFrame:
    required = {
        "timestamp",
        "source_ip",
        "destination_ip",
        "destination_port",
        "label",
        "is_attack",
    }
    missing = sorted(required - set(flows.columns))
    if missing:
        raise ValueError(f"Missing flow columns required for windowing: {missing}")

    frame = flows.copy()
    frame["window_start"] = frame["timestamp"].dt.floor("min")
    keys = ["source_ip", "window_start"]
    numeric = [
        "total_fwd_packets",
        "total_backward_packets",
        "total_length_of_fwd_packets",
        "total_length_of_bwd_packets",
        "flow_duration",
        "syn_flag_count",
        "rst_flag_count",
        "ack_flag_count",
        "psh_flag_count",
        "average_packet_size",
    ]
    frame[numeric] = frame[numeric].fillna(0.0)
    grouped = frame.groupby(keys, sort=True, observed=True)
    windows = grouped.agg(
        flow_count=("label", "size"),
        distinct_destination_ports=("destination_port", "nunique"),
        distinct_destination_ips=("destination_ip", "nunique"),
        total_fwd_packets=("total_fwd_packets", "sum"),
        total_bwd_packets=("total_backward_packets", "sum"),
        total_fwd_bytes=("total_length_of_fwd_packets", "sum"),
        total_bwd_bytes=("total_length_of_bwd_packets", "sum"),
        mean_flow_duration=("flow_duration", "mean"),
        syn_flag_sum=("syn_flag_count", "sum"),
        rst_flag_sum=("rst_flag_count", "sum"),
        ack_flag_sum=("ack_flag_count", "sum"),
        psh_flag_sum=("psh_flag_count", "sum"),
        mean_packet_size=("average_packet_size", "mean"),
        is_attack=("is_attack", "any"),
    )

    port_counts = frame.groupby(keys + ["destination_port"], observed=True).size().groupby(level=keys).max()
    ip_counts = frame.groupby(keys + ["destination_ip"], observed=True).size().groupby(level=keys).max()
    attack_types = grouped["label"].agg(
        lambda labels: tuple(sorted({str(label) for label in labels if label != "BENIGN"}))
    )
    windows["top_destination_port_flow_count"] = port_counts
    windows["top_destination_ip_flow_count"] = ip_counts
    windows["attack_types"] = attack_types
    windows = windows.reset_index()
    windows[FEATURE_COLUMNS] = windows[FEATURE_COLUMNS].astype(float)
    windows["is_attack"] = windows["is_attack"].astype(bool)
    assert_no_identity_leakage(FEATURE_COLUMNS)
    return windows[["source_ip", "window_start", "is_attack", "attack_types", *FEATURE_COLUMNS]]


def build_windows_for_files(paths: Iterable[Path], chunksize: int = 100_000) -> pd.DataFrame:
    parts = [aggregate_windows(load_file(path, chunksize=chunksize)) for path in paths]
    windows = pd.concat(parts, ignore_index=True).sort_values(["window_start", "source_ip"]).reset_index(drop=True)
    duplicate_keys = windows.duplicated(["source_ip", "window_start"], keep=False)
    if duplicate_keys.any():
        examples = windows.loc[duplicate_keys, ["source_ip", "window_start"]].head(3).to_dict("records")
        raise ValueError(f"Input files overlap the same source-IP windows: {examples}")
    return windows


def build_day_windows(day: str, root: Path | None = None, chunksize: int = 100_000) -> pd.DataFrame:
    return build_windows_for_files(paths_for_day(day, root), chunksize=chunksize)
