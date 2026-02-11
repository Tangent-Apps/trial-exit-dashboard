#!/usr/bin/env python3
"""
Daily orchestrator: downloads latest RC exports from GCS, runs analysis, updates index.
Called by GitHub Action on a daily cron schedule.

RC exports land as: {bucket}/{date}/transactions_{timestamp}.csv.gz
Each file is a separate app's export. We identify apps from the data content.
"""

import json
import os
import sys
import tempfile
from datetime import datetime
from pathlib import Path

from google.cloud import storage


def find_latest_date_folder(bucket):
    """Find the most recent date folder in the bucket."""
    # List all blobs to find date prefixes
    blobs = list(bucket.list_blobs(delimiter='/'))
    prefixes = list(bucket.list_blobs(delimiter='/'))

    # Get unique date prefixes
    date_folders = set()
    iterator = bucket.list_blobs(delimiter='/')
    # Consume the iterator to populate prefixes
    list(iterator)
    for prefix in iterator.prefixes:
        folder = prefix.rstrip('/')
        # Check if it looks like a date (YYYY-MM-DD)
        try:
            datetime.strptime(folder, '%Y-%m-%d')
            date_folders.add(folder)
        except ValueError:
            continue

    if not date_folders:
        return None

    return sorted(date_folders)[-1]  # Latest date


def find_export_files(bucket, date_folder):
    """Find all export CSV files in a date folder."""
    prefix = f"{date_folder}/"
    blobs = list(bucket.list_blobs(prefix=prefix))
    csv_blobs = [b for b in blobs if b.name.endswith('.csv.gz') or b.name.endswith('.csv')]
    return csv_blobs


def download_blob(blob, dest_path):
    """Download a GCS blob to a local path."""
    blob.download_to_filename(dest_path)
    size_mb = os.path.getsize(dest_path) / (1024 * 1024)
    print(f"  Downloaded {blob.name} ({size_mb:.1f} MB)")


def detect_app_from_file(filepath):
    """Read a CSV file and try to detect which app it belongs to."""
    import pandas as pd

    fp = str(filepath)
    compression = 'gzip' if fp.endswith('.gz') else None

    try:
        df = pd.read_csv(fp, sep=';', compression=compression, nrows=5)
        if len(df.columns) <= 1:
            df = pd.read_csv(fp, sep=',', compression=compression, nrows=5)
    except Exception as e:
        print(f"  Error reading {filepath}: {e}")
        return None, None

    df.columns = df.columns.str.strip().str.lower()

    # Try to find app/project identifier
    for col in ['project_name', 'app_name', 'project_id', 'app_id', 'rc_original_app_user_id']:
        if col in df.columns:
            val = df[col].dropna().iloc[0] if len(df[col].dropna()) > 0 else None
            if val and col in ['project_name', 'app_name']:
                return str(val).strip(), col
            elif val and col in ['project_id', 'app_id']:
                return str(val).strip(), col

    # Try to identify from product_identifier prefix
    if 'product_identifier' in df.columns:
        prod = df['product_identifier'].dropna().iloc[0] if len(df['product_identifier'].dropna()) > 0 else None
        if prod:
            return str(prod).strip(), 'product_identifier'

    return None, None


def match_app_to_config(detected_id, detected_col, config_apps):
    """Try to match a detected app identifier to our config."""
    if detected_id is None:
        return None

    detected_lower = detected_id.lower()

    for app in config_apps:
        # Match by project ID
        if app.get('rc_project_id', '').lower() in detected_lower or detected_lower in app.get('rc_project_id', '').lower():
            return app

        # Match by name
        if app['name'].lower() in detected_lower or detected_lower in app['name'].lower():
            return app

        # Match by slug
        if app['slug'].lower() in detected_lower or detected_lower in app['slug'].lower():
            return app

        # Match by product identifier prefix (e.g., com.tangent.girltalk)
        if app['slug'].replace('-', '') in detected_lower.replace('.', '').replace('-', ''):
            return app

    return None


def update_index(data_dir, config):
    """Regenerate data/index.json from the files on disk."""
    index_path = data_dir / 'index.json'

    app_names = {app['slug']: app['name'] for app in config['apps']}
    apps = [app['slug'] for app in config['apps']]

    data = {}
    for app_slug in apps:
        app_dir = data_dir / app_slug
        if not app_dir.exists():
            continue
        dates = sorted([f.stem for f in app_dir.glob('*.json')])
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
    repo_root = Path(__file__).parent.parent
    config_path = Path(__file__).parent / 'config.json'
    data_dir = repo_root / 'data'

    with open(config_path) as f:
        config = json.load(f)

    bucket_name = config['gcs_bucket']
    if not bucket_name:
        print("ERROR: gcs_bucket not set in config.json")
        sys.exit(1)

    # Init GCS client
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    today = datetime.utcnow().strftime('%Y-%m-%d')

    # Find the latest date folder
    latest_date = find_latest_date_folder(bucket)
    if not latest_date:
        print("No date folders found in bucket. Exports may not have run yet.")
        update_index(data_dir, config)
        sys.exit(0)

    print(f"Latest export date: {latest_date}")

    # Find all export files in that folder
    export_files = find_export_files(bucket, latest_date)
    if not export_files:
        print(f"No export files found in {latest_date}/")
        update_index(data_dir, config)
        sys.exit(0)

    print(f"Found {len(export_files)} export files")

    processed = 0
    unmatched = 0

    for blob in export_files:
        print(f"\n[Processing {blob.name}]")

        suffix = '.csv.gz' if blob.name.endswith('.gz') else '.csv'
        with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
            tmp_path = tmp.name

        try:
            download_blob(blob, tmp_path)

            # Detect which app this file belongs to
            detected_id, detected_col = detect_app_from_file(tmp_path)
            print(f"  Detected: {detected_id} (from {detected_col})")

            app_config = match_app_to_config(detected_id, detected_col, config['apps'])

            if app_config is None:
                print(f"  Could not match to any configured app, skipping")
                unmatched += 1
                continue

            app_name = app_config['name']
            app_slug = app_config['slug']
            print(f"  Matched to: {app_name} ({app_slug})")

            # Run analysis
            from analyze import generate_json
            result = generate_json(tmp_path, app_name=app_name)

            if result is None:
                print(f"  No trial data found")
                continue

            result['date'] = today
            result['export_file'] = blob.name
            result['export_date'] = latest_date

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
    update_index(data_dir, config)

    print(f"\n{'='*40}")
    print(f"Done: {processed} apps processed, {unmatched} unmatched files")

    # Don't fail when exports simply haven't landed yet (first few days)
    if processed == 0:
        print("No data processed — exports may not have landed yet. This is normal for the first run.")
        sys.exit(0)


if __name__ == '__main__':
    main()
