#!/usr/bin/env python3
"""
Trial Exit Analysis — Works with RC scheduled data export (transaction-level data).
Classifies trial users into: Converted, Not Converted (approximates cancel vs billing issue).
"""

import pandas as pd
import json
import sys
import argparse
from datetime import datetime, timedelta
from pathlib import Path


def load_transaction_data(filepath):
    """Load RC transaction export CSV (semicolon or comma separated, optionally gzipped)."""
    fp = str(filepath)
    compression = 'gzip' if fp.endswith('.gz') else None

    # Try semicolon first (RC default), fall back to comma
    try:
        df = pd.read_csv(fp, sep=';', compression=compression)
        if len(df.columns) <= 1:
            raise ValueError("Single column detected, wrong separator")
    except (ValueError, Exception):
        df = pd.read_csv(fp, sep=',', compression=compression)

    # Normalize column names (lowercase, strip whitespace)
    df.columns = df.columns.str.strip().str.lower()

    # Parse timestamps
    for col in ['start_time', 'end_time', 'purchase_date', 'event_timestamp']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    # Parse numeric fields
    for col in ['price_in_usd', 'price', 'revenue', 'renewal_number']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # Normalize boolean fields
    for col in ['is_trial_period', 'is_trial_conversion', 'is_auto_renewable', 'is_sandbox']:
        if col in df.columns:
            df[col] = df[col].astype(str).str.lower().isin(['true', '1', 'yes'])

    return df


def detect_app_name(df):
    """Try to detect app name from the data."""
    for col in ['project_name', 'app_name', 'project_id', 'app_id']:
        if col in df.columns:
            val = df[col].dropna().iloc[0] if len(df[col].dropna()) > 0 else None
            if val:
                return str(val).strip()
    return None


def get_user_id_col(df):
    """Find the user ID column."""
    candidates = ['rc_original_app_user_id', 'app_user_id', 'subscriber_id', 'user_id', 'customer_id']
    for col in candidates:
        if col in df.columns:
            return col
    # Fall back to first column that contains 'user' or 'subscriber'
    for col in df.columns:
        if 'user' in col or 'subscriber' in col:
            return col
    return df.columns[0]


def get_price_col(df):
    """Find the price/revenue column."""
    candidates = ['price_in_usd', 'price', 'revenue', 'amount']
    for col in candidates:
        if col in df.columns:
            return col
    return None


def get_product_col(df):
    """Find the product identifier column."""
    candidates = ['product_identifier', 'product_id', 'product', 'sku']
    for col in candidates:
        if col in df.columns:
            return col
    return None


def classify_users(df):
    """
    Classify each user's trial outcome from transaction data.

    Logic:
    - Trial user: has at least one transaction with is_trial_period = true
    - Converted: has a paid transaction (price > 0, not trial)
    - Still in trial: trial end_time > now, no paid transaction
    - Not converted: trial ended, no paid transaction
      - Approximation: if is_auto_renewable was false -> likely cancelled
      - If is_auto_renewable was true but no renewal -> likely billing issue
    """
    user_col = get_user_id_col(df)
    price_col = get_price_col(df)
    product_col = get_product_col(df)

    if price_col is None:
        print("  WARNING: No price column found, cannot determine conversions")
        return pd.DataFrame()

    # Filter out sandbox transactions
    if 'is_sandbox' in df.columns:
        df = df[~df['is_sandbox']].copy()

    now = datetime.utcnow()

    # Get trial transactions
    has_trial_col = 'is_trial_period' in df.columns
    if not has_trial_col:
        print("  WARNING: No is_trial_period column, trying renewal_number == 0")
        if 'renewal_number' in df.columns:
            trial_txns = df[df['renewal_number'] == 0]
        else:
            print("  ERROR: Cannot identify trial transactions")
            return pd.DataFrame()
    else:
        trial_txns = df[df['is_trial_period'] == True]

    if len(trial_txns) == 0:
        print("  No trial transactions found")
        return pd.DataFrame()

    # Get unique trial users
    trial_users = trial_txns[user_col].unique()
    print(f"  Found {len(trial_users)} trial users")

    # For each trial user, determine outcome
    results = []
    for user_id in trial_users:
        user_txns = df[df[user_col] == user_id]
        user_trial_txns = trial_txns[trial_txns[user_col] == user_id]

        # Get trial info
        latest_trial = user_trial_txns.sort_values('start_time' if 'start_time' in df.columns else df.columns[0]).iloc[-1]

        trial_start = latest_trial.get('start_time')
        trial_end = latest_trial.get('end_time')
        product = latest_trial.get(product_col) if product_col else 'unknown'

        # Check for paid transactions (non-trial with price > 0)
        non_trial_txns = user_txns[
            (~user_txns.get('is_trial_period', pd.Series([False]*len(user_txns)))) &
            (user_txns[price_col] > 0)
        ] if has_trial_col else user_txns[
            (user_txns['renewal_number'] > 0) &
            (user_txns[price_col] > 0)
        ] if 'renewal_number' in df.columns else pd.DataFrame()

        has_paid = len(non_trial_txns) > 0

        # Check for trial conversion flag
        has_conversion_flag = False
        if 'is_trial_conversion' in df.columns:
            has_conversion_flag = user_txns['is_trial_conversion'].any()

        # Determine outcome
        if has_paid or has_conversion_flag:
            outcome = 'Converted'
            total_spent = non_trial_txns[price_col].sum() if has_paid else 0
        elif trial_end is not None and pd.notna(trial_end) and trial_end > now:
            outcome = 'Still in Trial'
            total_spent = 0
        else:
            # Trial ended, didn't convert
            # Approximate: check is_auto_renewable
            auto_renew = latest_trial.get('is_auto_renewable', None)
            if auto_renew is False or auto_renew == 'false':
                outcome = 'Cancelled'  # User turned off auto-renew
            elif auto_renew is True or auto_renew == 'true':
                outcome = 'Billing Issue'  # Auto-renew was on but no renewal
            else:
                outcome = 'Cancelled'  # Default to cancelled if unknown
            total_spent = 0

        results.append({
            'user_id': user_id,
            'outcome': outcome,
            'product': product,
            'trial_start': trial_start,
            'trial_end': trial_end,
            'total_spent': total_spent,
        })

    return pd.DataFrame(results)


def compute_rates(classified_df):
    """Compute conversion/cancel/billing rates."""
    resolved = classified_df[classified_df['outcome'] != 'Still in Trial']
    total = len(resolved)

    if total == 0:
        return {
            'total_trials': len(classified_df),
            'resolved': 0,
            'in_trial': len(classified_df),
            'converted': 0, 'cancelled': 0, 'billing_issue': 0,
            'conversion_rate': None, 'cancel_rate': None, 'billing_rate': None
        }

    conv = len(resolved[resolved['outcome'] == 'Converted'])
    canc = len(resolved[resolved['outcome'] == 'Cancelled'])
    bill = len(resolved[resolved['outcome'] == 'Billing Issue'])
    in_trial = len(classified_df[classified_df['outcome'] == 'Still in Trial'])

    return {
        'total_trials': len(classified_df),
        'resolved': total,
        'in_trial': in_trial,
        'converted': conv,
        'cancelled': canc,
        'billing_issue': bill,
        'conversion_rate': round(conv / total, 4),
        'cancel_rate': round(canc / total, 4),
        'billing_rate': round(bill / total, 4),
    }


def weekly_breakdown(classified_df):
    """Break down by week of trial start."""
    df = classified_df.copy()
    df['trial_start'] = pd.to_datetime(df['trial_start'], errors='coerce')
    df = df.dropna(subset=['trial_start'])

    df['week'] = df['trial_start'].apply(
        lambda d: (d - timedelta(days=d.weekday())).date() if pd.notna(d) else None
    )
    df = df.dropna(subset=['week'])

    weeks = []
    for week, group in sorted(df.groupby('week')):
        rates = compute_rates(group)
        rates['week_start'] = str(week)
        weeks.append(rates)
    return weeks


def product_breakdown(classified_df):
    """Break down by product."""
    products = []
    for prod, group in classified_df.groupby('product'):
        if pd.isna(prod):
            continue
        rates = compute_rates(group)
        rates['product_id'] = str(prod)
        products.append(rates)
    return sorted(products, key=lambda x: x['resolved'], reverse=True)


def generate_json(input_file, app_name=None):
    """Run full analysis and return JSON-serializable dict."""
    df = load_transaction_data(input_file)
    print(f"  Loaded {len(df)} transactions from {input_file}")
    print(f"  Columns: {', '.join(df.columns[:15])}...")

    if not app_name:
        app_name = detect_app_name(df) or 'App'

    classified = classify_users(df)

    if len(classified) == 0:
        print(f"  No trial users found in transaction data")
        return None

    overall = compute_rates(classified)
    weeks = weekly_breakdown(classified)
    products = product_breakdown(classified)

    result = {
        'date': datetime.utcnow().strftime('%Y-%m-%d'),
        'app': app_name,
        'generated_at': datetime.utcnow().isoformat() + 'Z',
        'data_source': 'transaction_export',
        'overall': overall,
        'weekly_cohorts': weeks,
        'products': products,
    }

    return result


def main():
    parser = argparse.ArgumentParser(description='Trial Exit Analysis (Transaction Data) → JSON')
    parser.add_argument('input_file', help='Path to RC transaction export .csv or .csv.gz')
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

    o = result['overall']
    print(f"\n  === {result['app']} Summary ===")
    print(f"  Trials: {o['total_trials']} | Resolved: {o['resolved']} | In Trial: {o['in_trial']}")
    if o['resolved'] > 0:
        print(f"  Converted:  {o['conversion_rate']*100:.1f}%")
        print(f"  Cancelled:  {o['cancel_rate']*100:.1f}%")
        print(f"  Billing:    {o['billing_rate']*100:.1f}%")


if __name__ == '__main__':
    main()
