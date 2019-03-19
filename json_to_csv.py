import csv
import json

with open('relationships/liveoutloud_to_xz662/contact_rel.json') as f:
    data = json.load(f)

with open('relationships/liveoutloud_to_xz662/contact_rel.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['OldId', 'NewId'])

    for k, v in data.items():
        writer.writerow([k, v])
