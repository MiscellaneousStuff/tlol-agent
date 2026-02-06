import os
import sys
try:
    from isal import igzip as gzip
except ImportError:
    import gzip
import json
from collections import defaultdict

FILE_PATH = "D:/huggingface/maknee/12_22/batch_001.jsonl.gz"

def stream_first_match(file_path):
    with gzip.open(file_path, 'rt', encoding='utf-8') as f:
        return f.readline()

def analyze_unnamed(file_path):
    print(f"📂 Opening: {file_path}\n")
    
    line = stream_first_match(file_path)
    match = json.loads(line)
    events = match.get("events", [])
    
    replication_events = [e["Replication"] for e in events if "Replication" in e]
    
    # Target unnamed fields
    targets = [
        (8, 0),    # 132k - most frequent
        (32, 11),  # 26k
        (32, 4),   # 12.5k
        (32, 7),   # 9.4k
        (32, 8),   # 6.7k
        (4, 15),   # 4.2k
        (32, 5),   # 2.7k
        (4, 3),    # 2.5k
        (128, 3),  # 1.5k - new category
        (128, 1),  # 947 - new category
        (32, 6),   # 774
    ]
    
    # Collect values for each
    field_data = {t: [] for t in targets}
    
    for rep in replication_events:
        t = rep.get("time", 0)
        net_data = rep.get("net_id_to_replication_datas", {})
        
        for entity_id, entity_data in net_data.items():
            primary = entity_data.get("primary_index")
            secondary = entity_data.get("secondary_index", 0)
            key = (primary, secondary)
            
            if key in field_data:
                data = entity_data.get("data", {})
                dtype = list(data.keys())[0] if data else None
                value = list(data.values())[0] if data else None
                field_data[key].append({
                    "time": t,
                    "entity": entity_id,
                    "dtype": dtype,
                    "value": value
                })
    
    # Analyze each field
    print("=" * 80)
    print("UNNAMED FIELD ANALYSIS")
    print("=" * 80)
    
    for (primary, secondary), samples in field_data.items():
        if not samples:
            continue
        
        print(f"\n{'='*60}")
        print(f"({primary}, {secondary}) - {len(samples):,} samples")
        print(f"{'='*60}")
        
        # Data type
        dtypes = set(s["dtype"] for s in samples)
        print(f"Data types: {dtypes}")
        
        # Value statistics
        values = [s["value"] for s in samples if isinstance(s["value"], (int, float))]
        if values:
            print(f"Min: {min(values):.2f}")
            print(f"Max: {max(values):.2f}")
            print(f"Avg: {sum(values)/len(values):.2f}")
            
            # Unique values (if few)
            unique = set(values)
            if len(unique) <= 20:
                print(f"Unique values: {sorted(unique)}")
        
        # Sample values over time
        print(f"\nSample progression:")
        entity_samples = defaultdict(list)
        for s in samples:
            entity_samples[s["entity"]].append((s["time"], s["value"]))
        
        # Pick one entity with most samples
        top_entity = max(entity_samples.keys(), key=lambda e: len(entity_samples[e]))
        history = entity_samples[top_entity][:10]
        
        print(f"  Entity {top_entity}:")
        for t, v in history:
            print(f"    t={t:>7.1f}s: {v}")
        
        # Guess based on patterns
        print(f"\n💡 Guess: ", end="")
        
        if values:
            vmin, vmax, vavg = min(values), max(values), sum(values)/len(values)
            
            if vmax <= 1.0 and vmin >= 0:
                print("Percentage/ratio (0-1 range)")
            elif vmax <= 100 and vmin >= 0 and len(unique) < 20:
                print("Level/stacks/charges")
            elif 100 < vavg < 3000:
                print("HP/Mana/Resource value")
            elif vmax > 10000:
                print("Position coordinate or large resource")
            elif vmin < 0:
                print("Modifier (can be negative)")
            elif (8, 0) == (primary, secondary):
                print("Likely cooldown/timer (most frequent update)")
            else:
                print("Unknown")
        else:
            print("Non-numeric data")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        FILE_PATH = sys.argv[1]
    analyze_unnamed(FILE_PATH)