# pip install google-cloud-bigtable

from google.cloud import bigtable
from google.cloud.bigtable import row_filters
from collections import defaultdict
from datetime import datetime

PROJECT_ID  = "mythic-aloe-457912-d5"
INSTANCE_ID = "gds-bigtable"
TABLE_ID    = "customer_orders"

client   = bigtable.Client(project=PROJECT_ID, admin=True)
instance = client.instance(INSTANCE_ID)
table    = instance.table(TABLE_ID)

# ─── 1) INSERT DATA ──────────────────────────────────────────────────────────────
def insert_demo_rows():
    # rows = [
    #     ("order#1001", {"orders:customer": "John Doe", "orders:product": "Laptop", "orders:amount": "1200", "delivery:status": "Shipped"}),
    #     ("order#1002", {"orders:customer": "Aditi Sharma", "orders:product": "Smartphone", "orders:amount": "750", "delivery:status": "Pending"}),
    #     ("order#1003", {"orders:customer": "Rahul Kumar", "orders:product": "Tablet", "orders:amount": "450", "delivery:status": "Delivered"}),
    # ]

    rows = [
        ("order#1005", {"orders:customer": "Shashank", "orders:product": "Laptop", "delivery:status": "Shipped"})
    ]
    for key, data in rows:
        r = table.direct_row(key)
        for colfam_col, val in data.items():
            cf, col = colfam_col.split(":", 1)
            r.set_cell(cf, col, val)
        r.commit()
    print("Inserted demo rows")

# ─── 2) SCAN ALL ROWS ─────────────────────────────────────────────────────────────
def scan_all():
    print("\n--- Scan all rows ---")
    partial = table.read_rows()
    partial.consume_all()
    for row_key, row in partial.rows.items():
        print(row_key.decode(), {
            f"{cf}:{col.decode()}": cell[0].value.decode()
            for cf, cols in row.cells.items()
            for col, cell in cols.items()
        })

# ─── 3) KEY-BASED LOOKUP ─────────────────────────────────────────────────────────
def lookup_key(key):
    print(f"\n--- Lookup row `{key}` ---")
    row = table.read_row(key)
    if not row:
        print("Row not found")
        return
    print({
        f"{cf}:{col.decode()}": cells[0].value.decode()
        for cf, cols in row.cells.items()
        for col, cells in cols.items()
    })

# ─── 4) FILTER QUERY ─────────────────────────────────────────────────────────────
def filter_amount(threshold=700):
    print(f"\n--- Rows with orders:amount > {threshold} ---")
    # Bigtable cells are strings; we pull all and filter client-side
    partial = table.read_rows()
    partial.consume_all()
    for key, row in partial.rows.items():
        amts = row.cells.get("orders", {}).get(b"amount", [])
        if not amts:
            continue
        amount = int(amts[0].value.decode())
        if amount > threshold:
            print(key.decode(), amount)

# ─── 5) GROUP-BY SIMULATION ───────────────────────────────────────────────────────
def group_by_customer():
    print("\n--- Count orders per customer ---")
    counts = defaultdict(int)
    partial = table.read_rows()
    partial.consume_all()
    for key, row in partial.rows.items():
        cust_cells = row.cells.get("orders", {}).get(b"customer", [])
        if cust_cells:
            cust = cust_cells[0].value.decode()
            counts[cust] += 1
    for cust, cnt in counts.items():
        print(cust, cnt)

# ─── 6) UPDATE A CELL ─────────────────────────────────────────────────────────────
def update_amount(key, new_amount):
    print(f"\n--- Update orders:amount of `{key}` to {new_amount} ---")
    r = table.direct_row(key)
    r.set_cell("orders", "amount", str(new_amount))
    r.commit()

# ─── 7) CELL VERSION HISTORY ─────────────────────────────────────────────────────
def show_versions(key, column_family, qualifier, versions=5):
    """
    Print the last `versions` versions of a given cell.
    Works whether cell.timestamp is a datetime or an integer of microseconds.
    """
    print(f"\n--- Last {versions} versions of {column_family}:{qualifier} in `{key}` ---")
    filt = row_filters.CellsColumnLimitFilter(versions)
    row = table.read_row(key, filter_=filt)
    cells = row.cells[column_family].get(qualifier.encode(), [])
    for idx, cell in enumerate(cells, start=1):
        ts_val = cell.timestamp
        # If it's already a datetime, just isoformat it
        if isinstance(ts_val, datetime):
            ts = ts_val.isoformat()
        else:
            # Otherwise assume it’s an integer of microseconds
            ts = datetime.fromtimestamp(ts_val / 1e6).isoformat()
        print(f"Version {idx}: {cell.value.decode('utf-8')} @ {ts}")

# ─── 8) DELETE ROWS ─────────────────────────────────────────────────────
def delete_row(key):
    print(f"\n--- Deleting row `{key}` ---")
    r = table.direct_row(key)
    r.delete()      # deletes entire row
    r.commit()
    print("Deleted")

# ─── MAIN FLOW ───────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    insert_demo_rows()
    # scan_all()
    # lookup_key("order#1002")
    # filter_amount(700)
    # group_by_customer()
    # update_amount("order#1002", 1100)
    # show_versions("order#1001", "orders", "amount", versions=3)
    # delete_row("order#1003")