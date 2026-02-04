import glob
import gzip
import json
import os
from collections import Counter
from dataclasses import dataclass, field, asdict
from typing import List, Dict, Optional
import csv

@dataclass
class GameMetadata:
    """Metadata for a single game"""
    file_path: str
    game_index: int  # Index within the file
    patch: str
    
    # Timing
    duration_seconds: float = 0.0
    packet_count: int = 0
    
    # Champions
    champions: List[str] = field(default_factory=list)
    champion_count: int = 0
    
    # Event counts
    total_deaths: int = 0
    total_kills: int = 0
    total_spells_cast: int = 0
    total_basic_attacks: int = 0
    total_items_bought: int = 0
    total_damage_events: int = 0
    
    # Event type distribution
    event_types: Dict[str, int] = field(default_factory=dict)
    
    # Spells used (unique)
    unique_spells: List[str] = field(default_factory=list)
    
    # Quality indicators
    has_full_draft: bool = False  # 10 champions
    has_movement_data: bool = False
    has_combat_data: bool = False


def extract_game_metadata(events: List[dict], file_path: str, game_index: int) -> GameMetadata:
    """Extract metadata from a single game's events"""
    
    # Get patch from file path (e.g., "12_22" from path)
    patch = "unknown"
    for part in file_path.replace("\\", "/").split("/"):
        if part.startswith("12_") or part.startswith("13_") or part.startswith("14_"):
            patch = part
            break
    
    meta = GameMetadata(
        file_path=file_path,
        game_index=game_index,
        patch=patch
    )
    
    meta.packet_count = len(events)
    
    champions = set()
    spells = set()
    event_counts = Counter()
    max_time = 0.0
    
    for event in events:
        if not isinstance(event, dict):
            continue
            
        # Each event is {EventType: {data}}
        for event_type, data in event.items():
            event_counts[event_type] += 1
            
            # Get time
            if isinstance(data, dict):
                time = data.get('time', 0)
                if time > max_time:
                    max_time = time
            
            # Extract specific data
            if event_type == 'CreateHero':
                champ = data.get('champion')
                if champ:
                    champions.add(champ)
                    
            elif event_type == 'HeroDie':
                meta.total_deaths += 1
                meta.total_kills += 1  # Each death is a kill
                
            elif event_type == 'CastSpellAns':
                meta.total_spells_cast += 1
                spell = data.get('spell_name')
                if spell:
                    spells.add(spell)
                    
            elif event_type == 'BasicAttackPos':
                meta.total_basic_attacks += 1
                
            elif event_type == 'BuyItem':
                meta.total_items_bought += 1
                
            elif event_type == 'UnitApplyDamage':
                meta.total_damage_events += 1
                
            elif event_type in ('WaypointGroup', 'WaypointGroupWithSpeed'):
                meta.has_movement_data = True
    
    meta.duration_seconds = max_time
    meta.champions = list(champions)
    meta.champion_count = len(champions)
    meta.has_full_draft = len(champions) >= 10
    meta.has_combat_data = meta.total_damage_events > 0 or meta.total_basic_attacks > 0
    meta.event_types = dict(event_counts)
    meta.unique_spells = list(spells)
    
    return meta


def scan_files(file_pattern: str, max_games_per_file: Optional[int] = None, max_files: Optional[int] = None) -> List[GameMetadata]:
    """Scan files and extract metadata"""
    
    files = glob.glob(file_pattern)
    if max_files:
        files = files[:max_files]
    
    print(f"Found {len(files)} files to scan")
    
    all_metadata = []
    
    for file_idx, file_path in enumerate(files):
        print(f"[{file_idx+1}/{len(files)}] Scanning {os.path.basename(file_path)}...")
        
        try:
            with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                game_idx = 0
                for line in f:
                    if not line.strip():
                        continue
                    
                    try:
                        game_data = json.loads(line)
                        events = game_data.get('events', [])
                        
                        meta = extract_game_metadata(events, file_path, game_idx)
                        all_metadata.append(meta)
                        
                        game_idx += 1
                        if max_games_per_file and game_idx >= max_games_per_file:
                            break
                            
                    except json.JSONDecodeError:
                        continue
                        
        except Exception as e:
            print(f"  Error reading {file_path}: {e}")
            continue
    
    return all_metadata


def save_metadata_csv(metadata: List[GameMetadata], output_path: str):
    """Save metadata to CSV"""
    
    if not metadata:
        print("No metadata to save")
        return
    
    # Flatten for CSV
    rows = []
    for m in metadata:
        row = {
            'file_path': m.file_path,
            'game_index': m.game_index,
            'patch': m.patch,
            'duration_seconds': m.duration_seconds,
            'duration_minutes': round(m.duration_seconds / 60, 1),
            'packet_count': m.packet_count,
            'champions': '|'.join(m.champions),
            'champion_count': m.champion_count,
            'total_deaths': m.total_deaths,
            'total_spells_cast': m.total_spells_cast,
            'total_basic_attacks': m.total_basic_attacks,
            'total_items_bought': m.total_items_bought,
            'total_damage_events': m.total_damage_events,
            'has_full_draft': m.has_full_draft,
            'has_movement_data': m.has_movement_data,
            'has_combat_data': m.has_combat_data,
            'unique_spell_count': len(m.unique_spells),
        }
        rows.append(row)
    
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    
    print(f"Saved {len(rows)} games to {output_path}")


def save_metadata_json(metadata: List[GameMetadata], output_path: str):
    """Save full metadata to JSON"""
    
    data = [asdict(m) for m in metadata]
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)
    
    print(f"Saved {len(data)} games to {output_path}")


def print_summary(metadata: List[GameMetadata]):
    """Print summary statistics"""
    
    if not metadata:
        print("No games found")
        return
    
    print("\n" + "="*60)
    print("DATASET SUMMARY")
    print("="*60)
    
    print(f"\nTotal games: {len(metadata)}")
    
    # By patch
    patches = Counter(m.patch for m in metadata)
    print(f"\nBy patch:")
    for patch, count in sorted(patches.items()):
        print(f"  {patch}: {count} games")
    
    # Duration stats
    durations = [m.duration_seconds for m in metadata if m.duration_seconds > 0]
    if durations:
        print(f"\nGame duration:")
        print(f"  Min: {min(durations)/60:.1f} min")
        print(f"  Max: {max(durations)/60:.1f} min")
        print(f"  Avg: {sum(durations)/len(durations)/60:.1f} min")
    
    # Champion stats
    all_champs = []
    for m in metadata:
        all_champs.extend(m.champions)
    champ_counts = Counter(all_champs)
    print(f"\nTop 10 champions:")
    for champ, count in champ_counts.most_common(10):
        print(f"  {champ}: {count}")
    
    # Quality stats
    full_draft = sum(1 for m in metadata if m.has_full_draft)
    has_movement = sum(1 for m in metadata if m.has_movement_data)
    has_combat = sum(1 for m in metadata if m.has_combat_data)
    
    print(f"\nData quality:")
    print(f"  Full draft (10 champs): {full_draft}/{len(metadata)} ({100*full_draft/len(metadata):.1f}%)")
    print(f"  Has movement data: {has_movement}/{len(metadata)} ({100*has_movement/len(metadata):.1f}%)")
    print(f"  Has combat data: {has_combat}/{len(metadata)} ({100*has_combat/len(metadata):.1f}%)")
    
    # Event type distribution
    all_events = Counter()
    for m in metadata:
        all_events.update(m.event_types)
    print(f"\nEvent types (total across all games):")
    for event_type, count in all_events.most_common():
        print(f"  {event_type}: {count:,}")


if __name__ == "__main__":
    # Scan all files in directory
    metadata = scan_files(
        "D:/huggingface/maknee/12_22/*.jsonl.gz",
        max_games_per_file=1,  # All games per file
        max_files=1  # All files, or set to 2 for quick test
    )
    
    # Print summary
    print_summary(metadata)
    
    # Save to files
    save_metadata_csv(metadata, "game_metadata.csv")
    save_metadata_json(metadata, "game_metadata.json")