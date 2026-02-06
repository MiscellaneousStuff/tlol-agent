import glob
import os
import csv
import re
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed

# --- COMPRESSION LIBRARY SELECTOR ---
try:
    from isal import igzip as gzip
    print("🚀 Using: ISAL (Fastest)")
except ImportError:
    try:
        from indexed_gzip import IndexedGzipFile
        print("🚀 Using: Indexed_Gzip (Fast)")
        
        class FastGzip:
            def open(self, filename, mode='rb'):
                return IndexedGzipFile(filename)
        gzip = FastGzip()
    except ImportError:
        import gzip
        print("⚠️ Using: Standard Gzip (Slow - pip install isal or indexed_gzip)")

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False

# --- CONFIG ---
MAX_WORKERS = max(4, multiprocessing.cpu_count() - 2)

# Byte patterns - events
P_DEATH = b'"HeroDie"'
P_SPELL = b'"CastSpellAns"'
P_ATTACK = b'"BasicAttackPos"'
P_DAMAGE = b'"UnitApplyDamage"'
P_ITEM = b'"BuyItem"'
P_CREATE_HERO = b'"CreateHero"'
P_TIME = b'"time":'
RE_TIME = re.compile(rb'"time"\s*:\s*([\d.]+)')

# Byte patterns - field extraction
P_CHAMP_PRE = b'"champion":"'
P_NET_ID = b'"net_id":'
P_GOLD_ON_DEATH = b'"mBaseGoldGivenOnDeath"'
P_NEUTRAL_KILLS = b'"mNumNeutralMinionsKilled"'

# Regex for extracting values (compiled once)
RE_NET_ID = re.compile(rb'"net_id"\s*:\s*(\d+)')
RE_CHAMP_NAME = re.compile(rb'"champion"\s*:\s*"([^"]+)"')
RE_SKIN_NAME = re.compile(rb'"skin_name"\s*:\s*"([^"]+)"')

def new_game_stats():
    return {
        'deaths': 0, 'spells': 0, 'attacks': 0,
        'damage': 0, 'items': 0, 'champs': set(), 'size_bytes': 0,
        # New fields
        'hero_net_ids': {},       # net_id -> champion name
        'gold_on_death_events': [],  # list of (net_id, value)
        'neutral_kills_events': [],  # list of (net_id, value)
        'max_time': 0.0,  # Game duration
    }

def extract_create_hero_info(chunk):
    """Extract net_id and champion name from CreateHero events."""
    heroes = {}
    
    # Find all CreateHero blocks
    parts = chunk.split(P_CREATE_HERO)
    for part in parts[1:]:  # Skip first (before any CreateHero)
        # Look within a reasonable window (CreateHero events are ~500 bytes)
        window = part[:600]
        
        net_id_match = RE_NET_ID.search(window)
        champ_match = RE_CHAMP_NAME.search(window)
        
        if net_id_match:
            net_id = net_id_match.group(1).decode('utf-8')
            champ_name = ""
            
            if champ_match:
                champ_name = champ_match.group(1).decode('utf-8')
            else:
                # Fallback to skin_name if champion not found
                skin_match = RE_SKIN_NAME.search(window)
                if skin_match:
                    champ_name = skin_match.group(1).decode('utf-8')
            
            heroes[net_id] = champ_name
    
    return heroes

def extract_replication_field(chunk, field_pattern):
    """Extract net_id and value for a specific replication field."""
    results = []
    
    parts = chunk.split(field_pattern)
    for part in parts[1:]:
        # Look backwards for net_id (it's before the field name in the JSON)
        # The structure is: "net_id": { ... "name": "mFieldName", "data": {"Float": value} }
        # We need to search backwards in the chunk before this field
        
        # Get position of this field in original chunk
        # Instead, search in a window before and after
        idx = chunk.find(field_pattern)
        search_start = max(0, idx - 200) if idx > 0 else 0
        
    # Alternative approach: find the pattern and look for nearby net_id
    # Replication structure: "net_id_to_replication_datas": { "123456": { ... "name": "mField" ... } }
    
    # Search for pattern with context
    pos = 0
    while True:
        idx = chunk.find(field_pattern, pos)
        if idx == -1:
            break
        
        # Look backwards for the net_id (should be within ~300 bytes before)
        search_window = chunk[max(0, idx-300):idx]
        
        # Find the last quoted number (net_id) before this field
        # Pattern: "1073741859": { ... "name": "mFieldName"
        net_id_matches = list(re.finditer(rb'"(\d{9,12})"\s*:', search_window))
        
        if net_id_matches:
            net_id = net_id_matches[-1].group(1).decode('utf-8')
            
            # Look forward for the value
            value_window = chunk[idx:idx+100]
            value_match = re.search(rb'"Float"\s*:\s*([\d.]+)', value_window)
            
            if value_match:
                try:
                    value = float(value_match.group(1).decode('utf-8'))
                    results.append((net_id, value))
                except ValueError:
                    pass
        
        pos = idx + len(field_pattern)
    
    return results

def process_game_chunk(chunk, stats):
    """Process a chunk of data for one game, updating stats in place."""
    stats['size_bytes'] += len(chunk)
    stats['deaths'] += chunk.count(P_DEATH)
    stats['spells'] += chunk.count(P_SPELL)
    stats['attacks'] += chunk.count(P_ATTACK)
    stats['damage'] += chunk.count(P_DAMAGE)
    stats['items'] += chunk.count(P_ITEM)
    
    # Extract champion names (original method)
    if P_CHAMP_PRE in chunk:
        parts = chunk.split(P_CHAMP_PRE)
        for p in parts[1:]:
            end = p.find(b'"')
            if 0 < end < 30:
                try:
                    stats['champs'].add(p[:end].decode('utf-8'))
                except:
                    pass
    
    # Extract CreateHero net_id -> champion mapping
    if P_CREATE_HERO in chunk:
        heroes = extract_create_hero_info(chunk)
        stats['hero_net_ids'].update(heroes)
    
    # Extract mBaseGoldGivenOnDeath events (death bounty changes)
    if P_GOLD_ON_DEATH in chunk:
        gold_events = extract_replication_field(chunk, P_GOLD_ON_DEATH)
        stats['gold_on_death_events'].extend(gold_events)
    
    # Extract mNumNeutralMinionsKilled events
    if P_NEUTRAL_KILLS in chunk:
        neutral_events = extract_replication_field(chunk, P_NEUTRAL_KILLS)
        stats['neutral_kills_events'].extend(neutral_events)

    # Track max time (game duration)
    for match in RE_TIME.finditer(chunk):
        try:
            t = float(match.group(1).decode('utf-8'))
            if t > stats['max_time']:
                stats['max_time'] = t
        except ValueError:
            pass

def finalize_stats(stats, filename, patch, game_idx):
    """Convert stats to output row."""
    champs_str = "|".join(sorted(stats['champs']))
    
    # Format hero_net_ids as "netid:champ|netid:champ"
    hero_mapping = "|".join(f"{nid}:{name}" for nid, name in sorted(stats['hero_net_ids'].items()))
    
    # Get unique net_ids that had gold_on_death changes (likely deaths)
    death_net_ids = set(nid for nid, _ in stats['gold_on_death_events'])
    
    # Get max neutral kills per hero
    neutral_kills_by_hero = {}
    for nid, val in stats['neutral_kills_events']:
        neutral_kills_by_hero[nid] = max(neutral_kills_by_hero.get(nid, 0), val)
    
    neutral_kills_str = "|".join(f"{nid}:{int(val)}" for nid, val in sorted(neutral_kills_by_hero.items()))
    
    return {
        'file': filename,
        'game_idx': game_idx,
        'patch': patch,
        'size_mb': round(stats['size_bytes'] / (1024 * 1024), 2),
        'deaths': stats['deaths'],
        'spells': stats['spells'],
        'attacks': stats['attacks'],
        'damage': stats['damage'],
        'items': stats['items'],
        'n_champs': len(stats['champs']),
        'champs': champs_str,
        # New columns
        'hero_net_ids': hero_mapping,
        'n_heroes': len(stats['hero_net_ids']),
        'gold_death_net_ids': "|".join(sorted(death_net_ids)),
        'n_gold_death_events': len(stats['gold_on_death_events']),
        'neutral_kills': neutral_kills_str,
        # ... existing fields ...
        'duration_sec': round(stats['max_time'], 1),
        'duration_min': round(stats['max_time'] / 60, 2),
    }

def process_file_per_game(file_path):
    filename = os.path.basename(file_path)
    
    # Extract patch from path
    patch = "unknown"
    parts = file_path.replace("\\", "/").split("/")
    for p in parts:
        if p.startswith(("12_", "13_", "14_")):
            patch = p
            break

    results = []
    game_idx = 0
    current_stats = new_game_stats()
    buffer = b""
    
    CHUNK_SIZE = 8 * 1024 * 1024  # 8MB chunks

    try:
        with gzip.open(file_path, 'rb') as f:
            while True:
                chunk = f.read(CHUNK_SIZE)
                if not chunk:
                    break
                
                data = buffer + chunk
                lines = data.split(b'\n')
                buffer = lines[-1]
                complete_lines = lines[:-1]
                
                for line in complete_lines:
                    if not line.strip():
                        continue
                    
                    process_game_chunk(line, current_stats)
                    results.append(finalize_stats(current_stats, filename, patch, game_idx))
                    game_idx += 1
                    current_stats = new_game_stats()
            
            if buffer.strip():
                process_game_chunk(buffer, current_stats)
                results.append(finalize_stats(current_stats, filename, patch, game_idx))

    except Exception as e:
        return None, f"Error {filename}: {e}"
    
    return results, None

def main():
    pattern = "D:/huggingface/maknee/*/*.jsonl.gz"
    files = sorted(glob.glob(pattern))
    files = [files[0]]
    print(f"Found {len(files)} files.")
    
    all_rows = []
    
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_file_per_game, f): f for f in files}
        
        if HAS_TQDM:
            pbar = tqdm(total=len(files), unit="file", desc="Processing")
        
        for future in as_completed(futures):
            data, err = future.result()
            if data:
                all_rows.extend(data)
            elif err:
                print(f"\n{err}")
            
            if HAS_TQDM:
                pbar.update(1)
        
        if HAS_TQDM:
            pbar.close()

    if all_rows:
        keys = [
            'file', 'game_idx', 'patch', 'size_mb', 'duration_sec', 'duration_min',
            'deaths', 'spells', 'attacks', 'damage', 'items', 'n_champs', 'champs',
            'hero_net_ids', 'n_heroes', 'gold_death_net_ids', 
            'n_gold_death_events', 'neutral_kills'
        ]
        with open("metadata_per_game.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(all_rows)
        print(f"\n✅ Saved {len(all_rows)} games to metadata_per_game.csv")

if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()