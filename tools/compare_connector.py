#!/usr/bin/env python3
"""Time a snapshot or CDF source through the official Python connector."""

import json
import pathlib
import sys
import time

import delta_sharing
import importlib.metadata


if len(sys.argv) != 5:
    raise SystemExit(
        "Usage: compare_connector.py PROFILE TABLE LIMIT_OR_CDF ITERATIONS"
    )

profile = pathlib.Path(sys.argv[1]).expanduser().resolve()
table_name = sys.argv[2]
bound_text = sys.argv[3]
if bound_text.startswith("cdf:"):
    cdf_bounds = tuple(int(value) for value in bound_text.split(":")[1:])
    limit = None
else:
    cdf_bounds = None
    limit = None if bound_text == "none" else int(bound_text)
iterations = int(sys.argv[4])
if (
    not profile.is_file()
    or iterations < 1
    or (cdf_bounds is not None and len(cdf_bounds) != 2)
    or (limit is not None and limit < 0)
):
    raise SystemExit("PROFILE, LIMIT, or ITERATIONS is invalid.")

table_url = f"{profile}#{table_name}"
measurements = []
for iteration in range(1, iterations + 1):
    started = time.perf_counter()
    if cdf_bounds is None:
        data = delta_sharing.load_as_pandas(
            table_url,
            limit=limit,
            use_delta_format=True,
        )
    else:
        data = delta_sharing.load_table_changes_as_pandas(
            table_url,
            starting_version=cdf_bounds[0],
            ending_version=cdf_bounds[1],
            use_delta_format=True,
        )
    measurements.append(
        {
            "iteration": iteration,
            "elapsed_seconds": time.perf_counter() - started,
            "rows": len(data.index),
            "columns": len(data.columns),
        }
    )

print(
    json.dumps(
        {
            "connector": "delta-sharing Python",
            "connector_version": importlib.metadata.version("delta-sharing"),
            "table": table_name,
            "mode": "snapshot" if cdf_bounds is None else "changes",
            "limit": limit,
            "cdf_bounds": cdf_bounds,
            "measurements": measurements,
        },
        indent=2,
    )
)
