import json
import time
import argparse
import os
from datetime import datetime
from collections import defaultdict

import numpy as np
import matplotlib.pyplot as plt
from kafka import KafkaConsumer


def main():
    parser = argparse.ArgumentParser(description='Kafka transaction consumer with visualization')
    parser.add_argument('--bootstrap-servers', default='localhost:9092')
    parser.add_argument('--topic', default='transactions')
    parser.add_argument('--group-id', default='test-consumer-group')
    parser.add_argument('--timeout', type=int, default=120, help='Seconds to wait for messages')
    parser.add_argument('--output-dir', default='./output')
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    print(f"Connecting to Kafka at {args.bootstrap_servers}, topic '{args.topic}'...")

    consumer = KafkaConsumer(
        'transactions',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        # enable_auto_commit=True,
        # group_id=args.group_id,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=args.timeout * 1000,
    )

    transactions = []
    print(f"Consuming messages (timeout: {args.timeout}s)...")

    try:
        for message in consumer:
            transactions.append(message.value)
            if len(transactions) % 1000 == 0:
                print(f"  Consumed {len(transactions):,} messages...")
    except KeyboardInterrupt:
        print("\nStopped by user.")
    finally:
        consumer.close()

    print(f"Total consumed: {len(transactions):,} transactions.")

    if not transactions:
        print("No data to visualize.")
        return

    # Extract fields
    amounts = [t['amount'] for t in transactions]
    normal_amounts = [t['amount'] for t in transactions if not t.get('is_scam')]
    scam_amounts = [t['amount'] for t in transactions if t.get('is_scam')]
    scam_types = [t.get('scam_type') for t in transactions if t.get('is_scam')]
    lats = [t['gps']['latitude'] for t in transactions]
    lons = [t['gps']['longitude'] for t in transactions]
    scam_lats = [t['gps']['latitude'] for t in transactions if t.get('is_scam')]
    scam_lons = [t['gps']['longitude'] for t in transactions if t.get('is_scam')]

    hours = []
    daily = defaultdict(int)
    daily_scam = defaultdict(int)
    for t in transactions:
        try:
            ts = datetime.fromisoformat(t['timestamp'])
            hours.append(ts.hour)
            day = t['timestamp'][:10]
            daily[day] += 1
            if t.get('is_scam'):
                daily_scam[day] += 1
        except:
            pass

    # Visualize
    fig, axes = plt.subplots(3, 2, figsize=(16, 18))
    fig.suptitle(f'Transaction Analysis ({len(transactions):,} transactions)', fontsize=14, fontweight='bold')

    # 1. Amount distribution
    ax = axes[0, 0]
    ax.hist(normal_amounts, bins=50, alpha=0.6, label='Normal', color='steelblue', density=True)
    if scam_amounts:
        ax.hist(scam_amounts, bins=50, alpha=0.6, label='Scam', color='crimson', density=True)
    ax.set_title('Amount Distribution')
    ax.set_xlabel('Amount')
    ax.set_ylabel('Density')
    ax.legend()

    # 2. Scam type breakdown
    ax = axes[0, 1]
    if scam_types:
        type_counts = defaultdict(int)
        for st in scam_types:
            type_counts[st] += 1
        ax.bar(type_counts.keys(), type_counts.values(), color='crimson', alpha=0.7)
        ax.set_title(f'Scam Types (n={sum(type_counts.values()):,})')
        ax.set_ylabel('Count')
        ax.tick_params(axis='x', rotation=45)
    else:
        ax.text(0.5, 0.5, 'No scams detected', ha='center', va='center')
        ax.set_title('Scam Types')

    # 3. Hourly distribution
    ax = axes[1, 0]
    ax.hist(hours, bins=24, range=(0, 24), color='steelblue', alpha=0.7, edgecolor='white')
    ax.set_title('Transactions by Hour of Day')
    ax.set_xlabel('Hour')
    ax.set_ylabel('Count')

    # 4. GPS scatter
    ax = axes[1, 1]
    sample_n = min(len(lats), 10000)
    idx = np.random.choice(len(lats), sample_n, replace=False)
    ax.scatter([lons[i] for i in idx], [lats[i] for i in idx], s=1, alpha=0.2, color='steelblue')
    if scam_lats:
        ax.scatter(scam_lons, scam_lats, s=5, alpha=0.5, color='crimson', marker='x', label='Scam')
        ax.legend()
    ax.set_title('Transaction Locations')
    ax.set_xlabel('Longitude')
    ax.set_ylabel('Latitude')

    # 5. Daily volume
    ax = axes[2, 0]
    if daily:
        days = sorted(daily.keys())
        ax.bar(range(len(days)), [daily[d] for d in days], color='steelblue', alpha=0.7, label='Total')
        ax.bar(range(len(days)), [daily_scam.get(d, 0) for d in days], color='crimson', alpha=0.7, label='Scam')
        ax.set_title('Daily Transaction Volume')
        ax.set_xlabel('Day')
        ax.set_ylabel('Count')
        ax.legend()
        step = max(1, len(days) // 8)
        ax.set_xticks(range(0, len(days), step))
        ax.set_xticklabels([days[i][5:] for i in range(0, len(days), step)], rotation=45)

    # 6. Summary stats
    ax = axes[2, 1]
    ax.axis('off')
    scam_count = len(scam_amounts)
    stats_text = (
        f"Total transactions:  {len(transactions):,}\n"
        f"Normal:              {len(transactions) - scam_count:,}\n"
        f"Scam:                {scam_count:,} ({scam_count/len(transactions)*100:.1f}%)\n"
        f"Avg amount:          {np.mean(amounts):.2f}\n"
        f"Median amount:       {np.median(amounts):.2f}\n"
        f"Unique users:        {len(set(t['user_id'] for t in transactions)):,}\n"
        f"Unique cards:        {len(set(t['card_id'] for t in transactions)):,}\n"
    )
    ax.text(0.1, 0.5, stats_text, fontsize=12, fontfamily='monospace', va='center')
    ax.set_title('Summary')

    plt.tight_layout()
    path = os.path.join(args.output_dir, 'analysis.png')
    plt.savefig(path, dpi=150)
    plt.close()
    print(f"Visualization saved to {path}")


if __name__ == '__main__':
    main()