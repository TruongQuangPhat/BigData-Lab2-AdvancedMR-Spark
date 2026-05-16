
import csv
from collections import Counter

path = r'd:\Schools\BigData\Lab03\BigData-Lab2-AdvancedMR-Spark\data\Amazon Sale Report.csv'
with open(path, encoding='utf-8', errors='replace') as f:
    reader = csv.DictReader(f)
    rows = list(reader)

print(f"Total rows: {len(rows)}")
print(f"Columns: {list(rows[0].keys())}")
print()

# --- Date format check ---
date_formats = Counter()
for r in rows:
    d = r['Date'].strip()
    if not d:
        date_formats['EMPTY'] += 1
    elif len(d) == 8 and d[2] == '-' and d[5] == '-':
        date_formats['MM-DD-YY'] += 1
    else:
        date_formats['OTHER:' + d[:10]] += 1

print("Date format distribution:")
for k, v in date_formats.most_common(10):
    print(f"  {k}: {v}")
print()

# --- Amount nulls ---
empty_amount = [r for r in rows if not r['Amount'].strip()]
print(f"Empty Amount rows: {len(empty_amount)}")
sample_empty = empty_amount[:3]
for r in sample_empty:
    sku = r['SKU']
    date = r['Date']
    status = r['Status']
    promo = r['promotion-ids'][:50]
    print(f"  SKU={sku}, Date={date}, Status={status}, promo={repr(promo)}")
print()

# --- Amount zero check ---
zero_amount = [r for r in rows if r['Amount'].strip() == '0' or r['Amount'].strip() == '0.0']
print(f"Zero Amount rows: {len(zero_amount)}")
print()

# --- SKU null check ---
null_sku = sum(1 for r in rows if not r['SKU'].strip())
print(f"Empty SKU rows: {null_sku}")
print()

# --- promotion-ids analysis ---
promos_non_empty = [r['promotion-ids'] for r in rows if r['promotion-ids'].strip()]
print(f"Rows with promotions: {len(promos_non_empty)}")

# Count promotions per row
promo_counts = Counter()
for r in rows:
    p = r['promotion-ids'].strip()
    if not p:
        cnt = 0
    else:
        parts = [x for x in p.split(',') if x.strip()]
        cnt = len(parts)
    promo_counts[cnt] += 1

print("Distribution of promo_count per row (top 10):")
for k, v in sorted(promo_counts.items())[:10]:
    print(f"  promo_count={k}: {v} rows")
print()

# Check for spaces around promo IDs
has_spaces = sum(1 for p in promos_non_empty if p != p.strip())
print(f"promotion-ids with leading/trailing whitespace: {has_spaces}")
print()

# --- Check SKU-Month groups ---
from datetime import datetime
sku_month_groups = Counter()
date_errors = 0
for r in rows:
    date_str = r['Date'].strip()
    sku = r['SKU'].strip()
    try:
        dt = datetime.strptime(date_str, '%m-%d-%y')
        sku_month_groups[(sku, dt.month)] += 1
    except:
        date_errors += 1

print(f"Unique SKU-Month groups: {len(sku_month_groups)}")
print(f"Date parse errors: {date_errors}")
print()

# Top groups by order count
top_groups = sku_month_groups.most_common(10)
print("Top 10 SKU-Month groups by order count:")
for (sku, month), cnt in top_groups:
    print(f"  SKU={sku}, Month={month}: {cnt} orders")
print()

# How many groups have > 1000 orders
large_groups = [(k, v) for k, v in sku_month_groups.items() if v > 1000]
print(f"SKU-Month groups with > 1000 orders: {len(large_groups)}")
