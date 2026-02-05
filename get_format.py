import os
import sys
try:
    from isal import igzip as gzip
except ImportError:
    import gzip
import json
from collections import defaultdict

# --- CONFIG ---
FILE_PATH = "D:/huggingface/maknee/12_22/batch_001.jsonl.gz"
MAX_EVENTS_TO_SCAN = None  # Scan all events for accurate count

def stream_first_match(file_path):
    with gzip.open(file_path, 'rt', encoding='utf-8') as f:
        return f.readline()

def explore_structure(file_path):
    print(f"📂 Opening: {file_path}")
    print(f"📦 Compressed size: {os.path.getsize(file_path) / (1024*1024):.1f} MB\n")
    
    print("⏳ Reading first match...")
    line = stream_first_match(file_path)
    print(f"📏 First match JSON size: {len(line) / (1024*1024):.1f} MB\n")
    
    print("⏳ Parsing JSON...")
    match = json.loads(line)
    events = match.get("events", [])
    print(f"📊 Total events in match: {len(events):,}\n")
    
    # Count all packet types
    packet_counts = defaultdict(int)
    for event in events:
        for packet_type in event.keys():
            packet_counts[packet_type] += 1
    
    print("=" * 60)
    print("PACKET TYPE COUNTS")
    print("=" * 60)
    for ptype, count in sorted(packet_counts.items(), key=lambda x: -x[1]):
        print(f"  {ptype:<30} {count:>8,}")
    
    # --- HERO DIE ANALYSIS ---
    print("\n" + "=" * 60)
    print("💀 HERO DIE ANALYSIS")
    print("=" * 60)
    
    hero_die_events = [e["HeroDie"] for e in events if "HeroDie" in e]
    print(f"\nFound {len(hero_die_events)} HeroDie events (JSON parsed)")
    
    # Compare with byte counting
    byte_count = line.encode('utf-8').count(b'"HeroDie"')
    print(f"Byte pattern count for '\"HeroDie\"': {byte_count}")
    
    if hero_die_events:
        print("\nSample HeroDie events:")
        for i, death in enumerate(hero_die_events[:5]):
            print(f"\n  Death {i+1}: {json.dumps(death, indent=4)}")
        
        # Check for different death-related fields
        print("\n\nHeroDie field analysis:")
        all_keys = set()
        for death in hero_die_events:
            all_keys.update(death.keys())
        print(f"  All fields in HeroDie: {sorted(all_keys)}")
    
    # --- CHECK FOR OTHER DEATH-RELATED PACKETS ---
    print("\n" + "=" * 60)
    print("🔍 OTHER DEATH-RELATED PACKETS")
    print("=" * 60)
    
    death_related = ['NPCDieMapView', 'NPCDieMapViewBroadcast', 'Die', 'Death']
    for ptype in death_related:
        matching = [e[ptype] for e in events if ptype in e]
        if matching:
            print(f"\n{ptype}: {len(matching)} events")
            print(f"  Sample: {json.dumps(matching[0], indent=4)[:400]}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        FILE_PATH = sys.argv[1]
    explore_structure(FILE_PATH)