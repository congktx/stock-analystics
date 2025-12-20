import json

def load_json_file(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

    return {}

def save_json_file(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)
        return True
    return False