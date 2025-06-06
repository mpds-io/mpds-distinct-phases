import json
from collections import defaultdict

def read_jsonl(file_path):
    # mb change on polars dfrm?
    with open(file_path, 'r', encoding='utf-8') as f:
        return [json.loads(line) for line in f]

def group_by_phase(data):
    grouped = defaultdict(set)  
    for entry in data:
        if 'phase' in entry and 'phase_id' in entry:
            grouped[entry['phase']].add(entry['phase_id'])  
    return grouped

def fetch_duplicates(grouped_data):
    return [
        {'phase': phase, 'phase_ids': list(phase_ids)}  
        for phase, phase_ids in grouped_data.items()
        if len(phase_ids) > 1  
    ]

def find_duplicates_mpds(input_path, output_path):
    data = read_jsonl(input_path)
    grouped = group_by_phase(data)
    duplicates = fetch_duplicates(grouped)
    
    with open(output_path, 'w', encoding='utf-8') as f:
        for item in duplicates:
            f.write(json.dumps(item, ensure_ascii=False) + '\n')
            
if __name__ == "__main__":
    input_file = "./atomic_structures.jsonl"  
    output_path = "./duplicate_mpds.jsonl"
    find_duplicates_mpds(input_file, output_path)