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
    
    # --- SAMPLE FROM DIFFERENT GAME PHASES ---
    print("=" * 70)
    print("REPLICATION SAMPLES BY GAME PHASE")
    print("=" * 70)
    
    phases = [
        ("Early (0-5 min)", 0, 300),
        ("Mid (10-15 min)", 600, 900),
        ("Late (20-25 min)", 1200, 1500),
        ("Very Late (30+ min)", 1800, 9999),
    ]
    
    for phase_name, t_start, t_end in phases:
        phase_events = [r for r in replication_events if t_start <= r.get("time", 0) < t_end]
        if not phase_events:
            print(f"\n🔹 {phase_name}: No events")
            continue
        
        print(f"\n🔹 {phase_name}: {len(phase_events)} events")
        
        # Show one sample
        sample = phase_events[len(phase_events)//2]  # Middle of phase
        print(f"   Sample at t={sample.get('time', 0):.1f}s:")
        print(json.dumps(sample, indent=2)[:1500])
        if len(json.dumps(sample)) > 1500:
            print("   ... (truncated)")
    
    # --- FIELD ANALYSIS FROM LATE GAME ---
    print("\n" + "=" * 70)
    print("LATE GAME FIELD ANALYSIS (20+ mins)")
    print("=" * 70)
    
    late_events = [r for r in replication_events if r.get("time", 0) >= 1200]
    if not late_events:
        late_events = replication_events[-1000:]  # Fallback to last 1000
        print("(Using last 1000 events as fallback)")
    
    field_names = defaultdict(int)
    field_samples = {}
    
    for rep in late_events[:500]:
        for key, value in rep.items():
            if key == "time":
                continue
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, dict) and "name" in item:
                        fname = item["name"]
                        field_names[fname] += 1
                        if fname not in field_samples:
                            field_samples[fname] = item
    
    print(f"\n{'Field Name':<30} {'Count':>10}")
    print("-" * 42)
    for fname, count in sorted(field_names.items(), key=lambda x: -x[1]):
        print(f"{fname:<30} {count:>10,}")
    
    # --- TRACK HERO ENTITY LATE GAME ---
    print("\n" + "=" * 70)
    print("TRACKING HERO HEALTH OVER LATE GAME")
    print("=" * 70)
    
    # Find entity with health data
    hero_entities = set()
    for rep in late_events[:100]:
        for key, value in rep.items():
            if key == "time":
                continue
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, dict) and item.get("name") == "health":
                        hero_entities.add(key)
    
    if hero_entities:
        sample_hero = list(hero_entities)[0]
        print(f"\nTracking entity: {sample_hero}")
        
        for rep in late_events[::50][:10]:  # Every 50th event, 10 samples
            if sample_hero in rep:
                t = rep.get("time", 0)
                data = rep[sample_hero]
                health = next((d for d in data if isinstance(d, dict) and d.get("name") == "health"), None)
                print(f"  t={t:>7.1f}s: {health}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        FILE_PATH = sys.argv[1]
    explore_replication(FILE_PATH)