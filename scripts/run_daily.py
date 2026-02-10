#!/usr/bin/env python3
"""
Daily orchestrator: downloads latest RC exports from GCS, runs analysis, updates index.
Called by GitHub Action on a daily cron schedule.
"""

import json
import os
import sys
import tempfile
from datetime import datetime
from pathlib import Path

from google.cloud import storage


def find_latest_export(bucket, prefix):
    """Find the most recent RC export file in a GCS prefix."""
    blobs = list(bucket.list_blobs(prefix=prefix))
    csv_blobs = [b for b in blobs if b.name.endswith('.csv.gz') or b.name.endswith('.csv')]

    if not csv_blobs:
        return None

    # Sort by time_created descending to get the latest
    csv_blobs.sort(key=lambda b: b.time_created, reverse=True)
    return csv_blobs[0]


def download_blob(blob, dest_path):
    """Download a GCS blob to a local path."""
    blob.download_to_filename(dest_path)
    size_mb = os.path.getsize(dest_path) / (1024 * 1024)
    print(f"  Downloaded {blob.name} ({size_mb:.1f} MB)")


def update_index(data_dir):
    """Regenerate data/index.json from the files on disk."""
    index_path = data_dir / 'index.json'

    # Load config for app names
    config_path = Path(__file__).parent / 'config.json'
    with open(config_path) as f:
        config = json.load(f)

    app_names = {app['slug']: app['name'] for app in config['apps']}
    apps = [app['slug'] for app in config['apps']]

    data = {}
    for app_slug in apps:
        app_dir = data_dir / app_slug
        if not app_dir.exists():
            continue
        dates = sorted([
            f.stem for f in app_dir.glob('*.json')
        ])
        if dates:
            data[app_slug] = dates

    index = {
        'apps': apps,
        'app_names': app_names,
        'data': data,
        'last_updated': datetime.utcnow().isoformat() + 'Z',
    }

    with open(index_path, 'w') as f:
        json.dump(index, f, indent=2)

    print(f"Updated index: {len(data)} apps with data")


def main():
    # Paths
    repo_root = Path(__file__).parent.parent
    config_path = Path(__file__).parent / 'config.json'
    data_dir = repo_root / 'data'

    # Load config
    with open(config_path) as f:
        config = json.load(f)

    bucket_name = config['gcs_bucket']
    if not bucket_name:
        print("ERROR: gcs_bucket not set in config.json")
        sys.exit(1)

    # Init GCS client (uses GOOGLE_APPLICATION_CREDENTIALS env var)
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    today = datetime.utcnow().strftime('%Y-%m-%d')
    processed = 0
    errors = 0

    for app in config['apps']:
        app_name = app['name']
        app_slug = app['slug']
        rc_project_id = app.get('rc_project_id', '')

        if not rc_project_id:
            print(f"\n[SKIP] {app_name}: no rc_project_id configured")
            continue

        print(f"\n[{app_name}]")

        # RC scheduled exports land under: {project_id}/
        prefix = f"{rc_project_id}/"
        latest_blob = find_latest_export(bucket, prefix)

        if not latest_blob:
            print(f"  No export files found under gs://{bucket_name}/{prefix}")
            errors += 1
            continue

        # Download to temp file
        suffix = '.csv.gz' if latest_blob.name.endswith('.gz') else '.csv'
        with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
            tmp_path = tmp.name

        try:
            download_blob(latest_blob, tmp_path)

            # Run analysis
            from analyze import generate_json
            result = generate_json(tmp_path, app_name=app_name)

            if result is None:
                print(f"  No trial data found")
                errors += 1
                continue

            # Override date to today (the snapshot date)
            result['date'] = today
            result['export_file'] = latest_blob.name
            result['export_created'] = latest_blob.time_created.isoformat()

            # Save JSON
            output_dir = data_dir / app_slug
            output_dir.mkdir(parents=True, exist_ok=True)
            output_path = output_dir / f"{today}.json"

            with open(output_path, 'w') as f:
                json.dump(result, f, indent=2, default=str)

            print(f"  Saved {output_path}")
            processed += 1

        finally:
            os.unlink(tmp_path)

    # Update index
    update_index(data_dir)

    print(f"\n{'='*40}")
    print(f"Done: {processed} apps processed, {errors} errors")

    if errors > 0 and processed == 0:
        sys.exit(1)


if __name__ == '__main__':
    main()
