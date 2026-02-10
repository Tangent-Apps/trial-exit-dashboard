#!/usr/bin/env python3
"""
Trial Exit Analysis — Adapted for daily automated runs.
Reads RC customer export CSV, classifies trial exits, outputs JSON summary.
"""

import pandas as pd
import json
import sys
import argparse
from datetime import timedelta, date, datetime
from pathlib import Path


def load_data(filepath):
    """Load RC customer export CSV (semicolon-separated, optionally gzipped)."""
    fp = str(filepath)
    compression = 'gzip' if fp.endswith('.gz') else None
    df = pd.read_csv(fp, sep=';', compression=compression)

    ts_cols = [
        'trial_start_at', 'trial_end_at', 'most_recent_purchase_at',
        'most_recent_renewal_at', 'latest_expiration_at',
        'subscription_opt_out_at', 'trial_opt_out_at',
        'most_recent_billing_issues_at', 'first_purchase_at'
    ]
    for col in ts_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            df[col + '_dt'] = pd.to_datetime(df[col], unit='ms', errors='coerce')

    if 'trial_start_at_dt' in df.columns:
        df['trial_start_date'] = df['trial_start_at_dt'].dt.date

    if 'total_spent' in df.columns:
        df['total_spent'] = pd.to_numeric(df['total_spent'], errors='coerce').fillna(0)

    return df


def classify_trial_exit(row):
    """Classify a user's trial exit outcome. Priority order matters."""
    status = str(row.get('status', '')).lower()
    spent = row.get('total_spent', 0)
    if pd.isna(spent):
        spent = 0
    has_billing_ts = pd.notna(row.get('most_recent_billing_issues_at'))

    if status == 'free_trial':
        return 'Still in Trial'
    if spent > 0:
        return 'Converted'
    if 'billing_issue' in status or has_billing_ts:
        return 'Billing Issue'
    return 'Cancelled'


def compute_rates(df_subset):
    """Compute conversion/cancel/billing rates for a group of users."""
    resolved = df_subset[df_subset['outcome'] != 'Still in Trial']
    total = len(resolved)
    if total == 0:
        return {
            'total_trials': len(df_subset),
            'resolved': 0,
            'in_trial': len(df_subset),
            'converted': 0, 'cancelled': 0, 'billing_issue': 0,
            'conversion_rate': None, 'cancel_rate': None, 'billing_rate': None
        }

    conv = len(resolved[resolved['outcome'] == 'Converted'])
    canc = len(resolved[resolved['outcome'] == 'Cancelled'])
    bill = len(resolved[resolved['outcome'] == 'Billing Issue'])
    in_trial = len(df_subset[df_subset['outcome'] == 'Still in Trial'])

    return {
        'total_trials': len(df_subset),
        'resolved': total,
        'in_trial': in_trial,
        'converted': conv,
        'cancelled': canc,
        'billing_issue': bill,
        'conversion_rate': round(conv / total, 4),
        'cancel_rate': round(canc / total, 4),
        'billing_rate': round(bill / total, 4),
    }


def weekly_breakdown(df):
    """Break down trial exits by week of trial start."""
    def get_week_start(dt):
        if pd.isna(dt):
            return None
        d = dt if isinstance(dt, date) else dt.date()
        return d - timedelta(days=d.weekday())

    df = df.copy()
    df['week'] = df['trial_start_date'].apply(
        lambda d: get_week_start(d) if pd.notna(d) else None
    )
    df = df.dropna(subset=['week'])

    weeks = []
    for week, group in sorted(df.groupby('week')):
        rates = compute_rates(group)
        rates['week_start'] = str(week)
        weeks.append(rates)

    return weeks


def product_breakdown(df):
    """Break down trial exits by product."""
    if 'latest_product' not in df.columns:
        return []

    products = []
    for prod, group in df.groupby('latest_product'):
        if pd.isna(prod):
            continue
        rates = compute_rates(group)
        rates['product_id'] = str(prod)
        products.append(rates)

    return sorted(products, key=lambda x: x['resolved'], reverse=True)


def generate_json(input_file, app_name=None):
    """Run full analysis and return JSON-serializable dict."""
    df = load_data(input_file)
    print(f"  Loaded {len(df)} users from {input_file}")

    # Auto-detect app name
    if not app_name:
        if 'project_name' in df.columns:
            app_name = str(df['project_name'].iloc[0]).strip()
        elif 'app_name' in df.columns:
            app_name = str(df['app_name'].iloc[0]).strip()
        else:
            app_name = 'App'

    # Only analyze users who had a trial
    if 'trial_start_at' in df.columns:
        df = df[df['trial_start_at'].notna()]
        print(f"  {len(df)} users had a trial")

    if len(df) == 0:
        print(f"  WARNING: No trial users found")
        return None

    # Classify
    df['outcome'] = df.apply(classify_trial_exit, axis=1)

    # Compute
    overall = compute_rates(df)
    weeks = weekly_breakdown(df)
    products = product_breakdown(df)

    result = {
        'date': datetime.utcnow().strftime('%Y-%m-%d'),
        'app': app_name,
        'generated_at': datetime.utcnow().isoformat() + 'Z',
        'overall': overall,
        'weekly_cohorts': weeks,
        'products': products,
    }

    return result


def main():
    parser = argparse.ArgumentParser(description='Trial Exit Analysis → JSON')
    parser.add_argument('input_file', help='Path to RC export .csv or .csv.gz')
    parser.add_argument('--app-name', default=None, help='Override app name')
    parser.add_argument('--output', '-o', default=None, help='Output JSON path')
    args = parser.parse_args()

    result = generate_json(args.input_file, args.app_name)

    if result is None:
        print("No data to analyze")
        sys.exit(1)

    if args.output:
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        with open(args.output, 'w') as f:
            json.dump(result, f, indent=2, default=str)
        print(f"  Saved to {args.output}")
    else:
        print(json.dumps(result, indent=2, default=str))

    # Print summary
    o = result['overall']
    print(f"\n  === {result['app']} Summary ===")
    print(f"  Trials: {o['total_trials']} | Resolved: {o['resolved']} | In Trial: {o['in_trial']}")
    if o['resolved'] > 0:
        print(f"  Converted:  {o['conversion_rate']*100:.1f}%")
        print(f"  Cancelled:  {o['cancel_rate']*100:.1f}%")
        print(f"  Billing:    {o['billing_rate']*100:.1f}%")


if __name__ == '__main__':
    main()
