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
MAX_EVENTS_TO_SCAN = 5000  # Don't parse entire 100MB, just sample

def stream_first_match(file_path):
    """Read just the first line (one match) as a string."""
    with gzip.open(file_path, 'rt', encoding='utf-8') as f:
        return f.readline()

def explore_structure(file_path):
    print(f"📂 Opening: {file_path}")
    print(f"📦 Compressed size: {os.path.getsize(file_path) / (1024*1024):.1f} MB\n")
    
    # Read first match
    print("⏳ Reading first match (this may take a moment)...")
    line = stream_first_match(file_path)
    print(f"📏 First match JSON size: {len(line) / (1024*1024):.1f} MB\n")
    
    # Parse it
    print("⏳ Parsing JSON...")
    match = json.loads(line)
    events = match.get("events", [])
    print(f"📊 Total events in match: {len(events):,}\n")
    
    # Collect packet types and sample data
    packet_types = defaultdict(list)
    
    for i, event in enumerate(events[:MAX_EVENTS_TO_SCAN]):
        for packet_type, data in event.items():
            if len(packet_types[packet_type]) < 2:  # Keep 2 samples each
                packet_types[packet_type].append(data)
    
    # Print summary
    print("=" * 60)
    print("PACKET TYPES FOUND (with samples)")
    print("=" * 60)
    
    for ptype in sorted(packet_types.keys()):
        samples = packet_types[ptype]
        print(f"\n🔹 {ptype} ({len(samples)} sample(s))")
        print("-" * 40)
        
        for j, sample in enumerate(samples):
            # Pretty print but truncate long values
            sample_str = json.dumps(sample, indent=2)
            if len(sample_str) > 800:
                sample_str = sample_str[:800] + "\n  ... (truncated)"
            print(f"Sample {j+1}:")
            print(sample_str)
    
    # Specifically look for champion-related data
    print("\n" + "=" * 60)
    print("🎮 CHAMPION EXTRACTION ANALYSIS")
    print("=" * 60)
    
    # Find all CreateHero events
    create_hero_events = [
        e["CreateHero"] for e in events 
        if "CreateHero" in e
    ]
    
    if create_hero_events:
        print(f"\nFound {len(create_hero_events)} CreateHero events:")
        for i, hero in enumerate(create_hero_events):
            print(f"\n  Hero {i+1}: {json.dumps(hero, indent=4)}")
    else:
        print("\n⚠️ No CreateHero events found!")
        print("Searching for any field containing 'champion' or 'hero'...")
        
        # Deep search for champion-like fields
        for event in events[:10]:
            event_str = json.dumps(event).lower()
            if 'champion' in event_str or 'hero' in event_str or 'akali' in event_str:
                print(f"\nFound potential match:")
                print(json.dumps(event, indent=2)[:500])
                break

if __name__ == "__main__":
    if len(sys.argv) > 1:
        FILE_PATH = sys.argv[1]
    explore_structure(FILE_PATH)