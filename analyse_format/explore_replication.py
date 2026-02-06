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

def explore_replication(file_path):
    print(f"📂 Opening: {file_path}\n")
    
    line = stream_first_match(file_path)
    match = json.loads(line)
    events = match.get("events", [])
    
    replication_events = [e["Replication"] for e in events if "Replication" in e]
    print(f"📊 Found {len(replication_events):,} Replication events")
    
    # Get time range
    times = [r.get("time", 0) for r in replication_events]
    min_t, max_t = min(times), max(times)
    print(f"⏱️  Time range: {min_t:.1f}s - {max_t:.1f}s ({max_t/60:.1f} mins)\n")
    
    # --- ANALYZE STRUCTURE ---
    print("=" * 70)
    print("INDEX -> NAME MAPPING")
    print("=" * 70)
    
    # Build mapping: (primary_index, secondary_index) -> name
    index_to_name = {}
    index_counts = defaultdict(int)
    
    for rep in replication_events:
        net_data = rep.get("net_id_to_replication_datas", {})
        for entity_id, entity_data in net_data.items():
            primary = entity_data.get("primary_index")
            secondary = entity_data.get("secondary_index", 0)
            name = entity_data.get("name", "")
            
            key = (primary, secondary)
            index_counts[key] += 1
            
            if name and key not in index_to_name:
                index_to_name[key] = name
    
    print(f"\n{'Primary':<10} {'Secondary':<10} {'Name':<25} {'Count':>10}")
    print("-" * 60)
    for (primary, secondary), count in sorted(index_counts.items(), key=lambda x: -x[1])[:30]:
        name = index_to_name.get((primary, secondary), "(unnamed)")
        print(f"{primary:<10} {secondary:<10} {name:<25} {count:>10,}")
    
    # --- ALL NAMED FIELDS ---
    print("\n" + "=" * 70)
    print("ALL NAMED FIELDS FOUND")
    print("=" * 70)
    
    for (primary, secondary), name in sorted(index_to_name.items()):
        count = index_counts[(primary, secondary)]
        print(f"  ({primary:>2}, {secondary:>2}) -> {name:<25} ({count:,} occurrences)")
    
    # --- SAMPLE LATE GAME DATA ---
    print("\n" + "=" * 70)
    print(f"LATE GAME SAMPLES (last 10% of match, t>{max_t*0.9:.0f}s)")
    print("=" * 70)
    
    late_events = [r for r in replication_events if r.get("time", 0) > max_t * 0.9]
    
    for rep in late_events[:3]:
        print(f"\n⏱️  t={rep.get('time', 0):.1f}s:")
        net_data = rep.get("net_id_to_replication_datas", {})
        for entity_id, entity_data in list(net_data.items())[:5]:
            primary = entity_data.get("primary_index")
            secondary = entity_data.get("secondary_index", 0)
            name = entity_data.get("name") or index_to_name.get((primary, secondary), "?")
            data = entity_data.get("data", {})
            value = list(data.values())[0] if data else None
            print(f"    Entity {entity_id}: {name} = {value}")
    
    # --- TRACK SPECIFIC ENTITY ---
    print("\n" + "=" * 70)
    print("TRACKING HERO OVER TIME (by mHP field)")
    print("=" * 70)
    
    # Find entities that have mHP updates
    hp_entities = defaultdict(list)
    
    for rep in replication_events:
        t = rep.get("time", 0)
        net_data = rep.get("net_id_to_replication_datas", {})
        for entity_id, entity_data in net_data.items():
            name = entity_data.get("name", "")
            if name == "mHP":
                value = list(entity_data.get("data", {}).values())[0]
                hp_entities[entity_id].append((t, value))
    
    print(f"\nEntities with mHP updates: {len(hp_entities)}")
    
    for entity_id, hp_history in list(hp_entities.items())[:3]:
        print(f"\n  Entity {entity_id} ({len(hp_history)} HP updates):")
        # Show first, mid, last
        samples = [hp_history[0], hp_history[len(hp_history)//2], hp_history[-1]]
        for t, hp in samples:
            print(f"    t={t:>7.1f}s: HP={hp:.1f}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        FILE_PATH = sys.argv[1]
    explore_replication(FILE_PATH)