import json, csv, os

os.makedirs('csv_exports', exist_ok=True)

# 1. Transactions
with open('ecommerce_data/transactions.json') as f:
    txns = json.load(f)
with open('csv_exports/transactions.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=['transaction_id','user_id','timestamp','total','subtotal','discount','status','payment_method'])
    writer.writeheader()
    for t in txns:
        writer.writerow({k: t.get(k,'') for k in ['transaction_id','user_id','timestamp','total','subtotal','discount','status','payment_method']})

# 2. Users
with open('ecommerce_data/users.json') as f:
    users = json.load(f)
with open('csv_exports/users.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=['user_id','city','state','country','registration_date','last_active'])
    writer.writeheader()
    for u in users:
        writer.writerow({
            'user_id': u['user_id'],
            'city':    u['geo_data']['city'],
            'state':   u['geo_data']['state'],
            'country': u['geo_data']['country'],
            'registration_date': u['registration_date'],
            'last_active':       u['last_active']
        })

# 3. Products
with open('ecommerce_data/products.json') as f:
    prods = json.load(f)
with open('csv_exports/products.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=['product_id','name','category_id','subcategory_id','base_price','current_stock','is_active'])
    writer.writeheader()
    for p in prods:
        writer.writerow({k: p.get(k,'') for k in ['product_id','name','category_id','subcategory_id','base_price','current_stock','is_active']})

# 4. Sessions summary
sessions = []
for i in range(4):
    with open(f'ecommerce_data/sessions_{i}.json') as f:
        sessions.extend(json.load(f))
with open('csv_exports/sessions.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=['session_id','user_id','start_time','duration_seconds','device_type','os','referrer','conversion_status'])
    writer.writeheader()
    for s in sessions:
        writer.writerow({
            'session_id':        s['session_id'],
            'user_id':           s['user_id'],
            'start_time':        s['start_time'],
            'duration_seconds':  s['duration_seconds'],
            'device_type':       s['device_profile']['type'],
            'os':                s['device_profile']['os'],
            'referrer':          s.get('referrer',''),
            'conversion_status': s.get('conversion_status','')
        })

print("All CSV files saved to csv_exports/ folder:")
for f in os.listdir('csv_exports'):
    print(f"  {f}")