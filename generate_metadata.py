import glob
import gzip
import os
from collections import Counter
from dataclasses import dataclass, field, asdict
from typing import List, Dict, Optional
import csv

# try:
#     import orjson as json
#     def loads(x): return json.loads(x)
# except ImportError:
#     import json
#     def loads(x): return json.loads(x)
#     print("pip install orjson for 10x faster parsing")

import json

from tqdm import tqdm


@dataclass 
class GameMetadata:
    file_path: str
    game_index: int
    patch: str
    duration_seconds: float = 0.0
    packet_count: int = 0
    champions: List[str] = field(default_factory=list)
    champion_count: int = 0
    total_deaths: int = 0
    total_spells_cast: int = 0
    total_basic_attacks: int = 0
    total_items_bought: int = 0
    total_damage_events: int = 0
    event_types: Dict[str, int] = field(default_factory=dict)
    has_full_draft: bool = False
    has_movement_data: bool = False
    has_combat_data: bool = False


def extract_metadata_fast(events: list, file_path: str, game_index: int, patch: str) -> GameMetadata:
    """Minimal extraction - only what we need"""
    
    meta = GameMetadata(file_path=file_path, game_index=game_index, patch=patch)
    meta.packet_count = len(events)
    
    champions = set()
    event_counts = Counter()
    max_time = 0.0
    
    for event in events:
        for event_type, data in event.items():
            event_counts[event_type] += 1
            
            t = data.get('time', 0) if isinstance(data, dict) else 0
            if t > max_time:
                max_time = t
            
            if event_type == 'CreateHero':
                c = data.get('champion')
                if c: champions.add(c)
            elif event_type == 'HeroDie':
                meta.total_deaths += 1
            elif event_type == 'CastSpellAns':
                meta.total_spells_cast += 1
            elif event_type == 'BasicAttackPos':
                meta.total_basic_attacks += 1
            elif event_type == 'BuyItem':
                meta.total_items_bought += 1
            elif event_type == 'UnitApplyDamage':
                meta.total_damage_events += 1
            elif event_type == 'WaypointGroup':
                meta.has_movement_data = True
    
    meta.duration_seconds = max_time
    meta.champions = list(champions)
    meta.champion_count = len(champions)
    meta.has_full_draft = len(champions) >= 10
    meta.has_combat_data = meta.total_damage_events > 0
    meta.event_types = dict(event_counts)
    return meta


def scan_files(file_pattern: str, max_games: Optional[int] = None) -> List[GameMetadata]:
    files = sorted(glob.glob(file_pattern))
    print(f"Found {len(files)} files")
    
    all_metadata = []
    total_games = 0
    
    for file_path in tqdm(files, desc="Files"):
        filename = os.path.basename(file_path)
        
        # Get patch from path
        patch = "unknown"
        for part in file_path.replace("\\", "/").split("/"):
            if part.startswith("12_") or part.startswith("13_") or part.startswith("14_"):
                patch = part
                break
        
        with gzip.open(file_path, 'rt', encoding='utf-8') as f:
            for game_idx, line in enumerate(f):
                if max_games and total_games >= max_games:
                    break
                if not line.strip():
                    continue
                
                game_data = loads(line)
                events = game_data.get('events', [])
                meta = extract_metadata_fast(events, filename, game_idx, patch)
                all_metadata.append(meta)
                total_games += 1
        
        if max_games and total_games >= max_games:
            break
    
    print(f"Processed {total_games} games")
    return all_metadata


def save_csv(metadata: List[GameMetadata], path: str):
    with open(path, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(['file', 'idx', 'patch', 'duration_min', 'packets', 'champions', 'deaths', 'spells', 'attacks', 'items', 'damage_events', 'full_draft'])
        for m in metadata:
            w.writerow([
                m.file_path, m.game_index, m.patch,
                round(m.duration_seconds/60, 1), m.packet_count,
                '|'.join(m.champions), m.total_deaths, m.total_spells_cast,
                m.total_basic_attacks, m.total_items_bought, m.total_damage_events,
                m.has_full_draft
            ])
    print(f"Saved to {path}")

if __name__ == "__main__":
    metadata = scan_files("D:/huggingface/maknee/*/*.jsonl.gz")
    save_csv(metadata, "game_metadata.csv")