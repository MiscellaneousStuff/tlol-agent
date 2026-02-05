import glob
import gzip
import os
import csv
import cProfile
import pstats
import io
from collections import Counter
from dataclasses import dataclass, field
from typing import List, Dict, Optional

# --- FIX IMPORTS ---
try:
    import orjson
    print("✅ Using orjson (Fast)")
    def loads(x): return orjson.loads(x)
except ImportError:
    import json
    print("⚠️ Using standard json (Slow - pip install orjson to fix)")
    def loads(x): return json.loads(x)

@dataclass 
class GameMetadata:
    file_path: str
    game_index: int
    patch: str
    duration_seconds: float = 0.0
    packet_count: int = 0
    champions: List[str] = field(default_factory=list)
    total_deaths: int = 0
    total_damage_events: int = 0
    has_full_draft: bool = False

def extract_metadata_fast(events: list, file_path: str, game_index: int, patch: str) -> GameMetadata:
    meta = GameMetadata(file_path=file_path, game_index=game_index, patch=patch)
    meta.packet_count = len(events)
    
    champions = set()
    max_time = 0.0
    
    # --- HOT LOOP START ---
    for event in events:
        for event_type, data in event.items():
            # Optimize: Get time only if needed (saves dict lookups)
            if isinstance(data, dict):
                t = data.get('time', 0)
                if t > max_time:
                    max_time = t
            
            # Optimize: String comparisons are fast, but millions of them add up
            if event_type == 'CreateHero':
                c = data.get('champion')
                if c: champions.add(c)
            elif event_type == 'HeroDie':
                meta.total_deaths += 1
            elif event_type == 'UnitApplyDamage':
                meta.total_damage_events += 1
    # --- HOT LOOP END ---

    meta.duration_seconds = max_time
    meta.champions = list(champions)
    meta.has_full_draft = len(champions) >= 10
    return meta

def scan_files(file_pattern: str, max_games: Optional[int] = None) -> List[GameMetadata]:
    files = sorted(glob.glob(file_pattern))
    # LIMIT TO 1 FILE FOR PROFILING
    if len(files) > 1:
        print(f"Found {len(files)} files, but profiling ONLY the first one.")
        files = [files[0]]
    
    all_metadata = []
    total_games = 0
    
    for file_path in files:
        filename = os.path.basename(file_path)
        print(f"Profiling file: {filename}")
        
        patch = "12_22" # simplified for profiling
        
        with gzip.open(file_path, 'rt', encoding='utf-8') as f:
            for game_idx, line in enumerate(f):
                if not line.strip():
                    continue
                
                # This is the heavy lifter
                game_data = loads(line)
                
                events = game_data.get('events', [])
                meta = extract_metadata_fast(events, filename, game_idx, patch)
                all_metadata.append(meta)
                total_games += 1
                
                if max_games and total_games >= max_games:
                    break
                    
    return all_metadata

if __name__ == "__main__":
    # --- PROFILER SETUP ---
    pr = cProfile.Profile()
    pr.enable()
    
    # Run the workload (Limit to 50 games so it finishes fast)
    scan_files("D:/huggingface/maknee/12_22/*.jsonl.gz", max_games=50)
    
    pr.disable()
    
    # --- PRINT RESULTS ---
    s = io.StringIO()
    ps = pstats.Stats(pr, stream=s).sort_stats('tottime') # Sort by internal time
    ps.print_stats(20) # Print top 20 lines
    
    print("\n" + "="*40)
    print("       PROFILING RESULTS       ")
    print("="*40)
    print(s.getvalue())