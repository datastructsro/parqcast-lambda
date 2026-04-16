"""
Demo Lambda: generates sensor data as Parquet, uploads raw bytes to parqcast-server.
"""

import io
import os
import random
import time
from urllib import request as urllib_request
from urllib.error import HTTPError, URLError

import pyarrow as pa
import pyarrow.parquet as pq


def lambda_handler(event, context):
    """Generate N segments of sensor data as Parquet, upload each to parqcast-server."""
    server_url = os.environ.get("PARQCAST_SERVER_URL", "http://127.0.0.1:8420")
    api_key = os.environ.get("PARQCAST_API_KEY", "")
    namespace = event.get("namespace", os.environ.get("NAMESPACE", "demo"))
    table = event.get("table", os.environ.get("TABLE", "sensor_readings"))
    segments = int(event.get("segments", os.environ.get("SEGMENTS", "5")))
    records_per = int(event.get("records_per_segment", os.environ.get("RECORDS_PER_SEGMENT", "10")))

    print(f"Config: {segments} segments x {records_per} records -> {server_url}/upload/{namespace}/{table}")

    sensors = ["temp_warehouse_A", "temp_warehouse_B", "humidity_dock",
               "vibration_conveyor_1", "pressure_line_3"]
    results = []
    failed = []

    for seg in range(segments):
        # Generate segment data
        base_ts = int(time.time()) + seg * 60
        seg_ids = []
        rec_indices = []
        timestamps = []
        sensor_ids = []
        values = []
        units = []
        zones = []

        for i in range(records_per):
            sensor = sensors[i % len(sensors)]
            seg_ids.append(seg)
            rec_indices.append(i)
            timestamps.append(base_ts + i)
            sensor_ids.append(sensor)
            values.append(round(random.uniform(10.0, 95.0), 2))
            units.append("celsius" if "temp" in sensor else "pct")
            zones.append(f"zone_{(seg % 3) + 1}")

        # Build Parquet in memory
        arrow_table = pa.table({
            "segment": pa.array(seg_ids, type=pa.int32()),
            "record_index": pa.array(rec_indices, type=pa.int32()),
            "timestamp": pa.array(timestamps, type=pa.int64()),
            "sensor_id": pa.array(sensor_ids, type=pa.string()),
            "value": pa.array(values, type=pa.float64()),
            "unit": pa.array(units, type=pa.string()),
            "zone": pa.array(zones, type=pa.string()),
        })
        buf = io.BytesIO()
        pq.write_table(arrow_table, buf, compression="snappy")
        parquet_bytes = buf.getvalue()

        # Upload raw Parquet bytes
        url = f"{server_url}/upload/{namespace}/{table}"
        headers = {"Content-Type": "application/octet-stream", "X-API-Key": api_key}

        uploaded = False
        for attempt in range(3):
            try:
                req = urllib_request.Request(url, data=parquet_bytes, headers=headers, method="POST")
                with urllib_request.urlopen(req, timeout=10) as resp:
                    import json
                    body = json.loads(resp.read().decode())
                results.append({
                    "segment": seg,
                    "upload_id": body["upload_id"],
                    "path": body["path"],
                    "rows": body["rows"],
                })
                print(f"  [{seg}/{segments}] OK -> {body['path']} ({body['rows']} rows, {body['size_bytes']} bytes)")
                uploaded = True
                break
            except (URLError, HTTPError) as e:
                print(f"  [{seg}/{segments}] attempt {attempt + 1} failed: {e}")
                time.sleep(0.5)

        if not uploaded:
            failed.append(seg)
            print(f"  [{seg}/{segments}] FAILED")

    summary = {
        "status": "complete" if not failed else "partial",
        "segments_uploaded": len(results),
        "segments_failed": len(failed),
        "total_records": sum(r["rows"] for r in results),
        "upload_ids": [r["upload_id"] for r in results],
        "paths": [r["path"] for r in results],
    }

    print(f"\nDone: {len(results)}/{segments} segments uploaded")
    return {"statusCode": 200 if not failed else 207, "body": summary}
