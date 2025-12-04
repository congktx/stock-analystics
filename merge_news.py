import json
import csv
import glob
import os
from tempfile import NamedTemporaryFile
import shutil
import config

OUTPUT_CSV = "NEWS_merged.csv"
CHECKPOINT = "NEWS_checkpoint.txt"

processed = set()
if os.path.exists(CHECKPOINT):
    with open(CHECKPOINT) as f:
        processed = set(line.strip() for line in f)

json_files = sorted(glob.glob(f'{config.DATA_JSON_PATH}\*.json'))
print("Total JSON:", len(json_files), "- already processed:", len(processed))

header = []
if os.path.exists(OUTPUT_CSV):
    with open(OUTPUT_CSV, encoding="utf-8") as f:
        header = f.readline().strip().split(",")

def add_new_columns(new_cols):
    global header

    missing = [c for c in new_cols if c not in header]
    if not missing:
        return False

    print("New columns detected:", missing)
    header += missing

    tmp = NamedTemporaryFile(delete=False, mode="w", newline="", encoding="utf-8")
    with open(OUTPUT_CSV, encoding="utf-8") as old, tmp:
        rows = old.readlines()
        if len(rows) == 0:
            tmp.write(",".join(header) + "\n")
        else:
            rows[0] = ",".join(header) + "\n"
            tmp.writelines(rows)

    shutil.move(tmp.name, OUTPUT_CSV)
    return True


first_write = not os.path.exists(OUTPUT_CSV)
cnt_obj = 560654

with open(OUTPUT_CSV, "a", newline="", encoding="utf-8") as csvfile:

    writer = None
    if header:
        writer = csv.DictWriter(csvfile, fieldnames=header)

    for jf in json_files:
        if jf in processed:
            continue

        print("Processing", jf)
        data = json.load(open(jf, encoding="utf-8"))
        if isinstance(data, dict):
            data = [data]
        # data = data[0]

        all_fields = set().union(*(obj.keys() for obj in data))
        if add_new_columns(all_fields):
            csvfile.close()
            csvfile = open(OUTPUT_CSV, "a", newline="", encoding="utf-8")
            writer = csv.DictWriter(csvfile, fieldnames=header)

        if first_write:
            writer = csv.DictWriter(csvfile, fieldnames=header)
            writer.writeheader()
            first_write = False

        for row in data:
          try:
            row['_id'] = cnt_obj
            writer.writerow({k: row.get(k, "") for k in header})
            cnt_obj += 1
          except Exception as e:
            print('error: ', row)

        with open(CHECKPOINT, "a") as ck:
            ck.write(jf + "\n")

print("DONE", cnt_obj)
