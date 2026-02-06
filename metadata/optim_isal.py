import glob
import os
import csv
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

# Byte patterns
P_DEATH = b'"HeroDie"'
P_SPELL = b'"CastSpellAns"'
P_ATTACK = b'"BasicAttackPos"'
P_DAMAGE = b'"UnitApplyDamage"'
P_ITEM = b'"BuyItem"'
P_CHAMP_PRE = b'"champion":"'
P_TIME = b'"time":'
P_CREATE_HERO = b'"CreateHero"'
P_NET_ID = b'"net_id":'

def new_game_stats():
    return {
        'deaths': 0, 'spells': 0, 'attacks': 0,
        'damage': 0, 'items': 0, 'champs': set(), 'size_bytes': 0,
        'max_time': 0.0,
        'hero_net_ids': {},  # net_id -> champ
    }

def extract_float_after(data, start_pos, max_len=20):
    """Fast float extraction without regex."""
    end = start_pos + max_len
    num_bytes = []
    for i in range(start_pos, min(end, len(data))):
        c = data[i:i+1]
        if c in b'0123456789.':
            num_bytes.append(c)
        elif num_bytes:  # Hit non-digit after starting number
            break
    if num_bytes:
        try:
            return float(b''.join(num_bytes))
        except:
            pass
    return None

def process_game_chunk(chunk, stats):
    """Process a chunk of data for one game, updating stats in place."""
    stats['size_bytes'] += len(chunk)
    stats['deaths'] += chunk.count(P_DEATH)
    stats['spells'] += chunk.count(P_SPELL)
    stats['attacks'] += chunk.count(P_ATTACK)
    stats['damage'] += chunk.count(P_DAMAGE)
    stats['items'] += chunk.count(P_ITEM)
    
    # Extract champion names
    if P_CHAMP_PRE in chunk:
        parts = chunk.split(P_CHAMP_PRE)
        for p in parts[1:]:
            end = p.find(b'"')
            if 0 < end < 30:
                try:
                    stats['champs'].add(p[:end].decode('utf-8'))
                except:
                    pass
    
    # Get last time value (game duration) - search from end
    last_time_pos = chunk.rfind(P_TIME)
    if last_time_pos != -1:
        t = extract_float_after(chunk, last_time_pos + len(P_TIME))
        if t and t > stats['max_time']:
            stats['max_time'] = t
    
    # Extract CreateHero net_id -> champion (fast version)
    if P_CREATE_HERO in chunk:
        parts = chunk.split(P_CREATE_HERO)
        for part in parts[1:]:
            window = part[:500]
            # Find net_id
            nid_pos = window.find(P_NET_ID)
            if nid_pos != -1:
                # Extract number after "net_id":
                nid_start = nid_pos + len(P_NET_ID)
                nid_end = nid_start
                while nid_end < len(window) and window[nid_end:nid_end+1] in b'0123456789 ':
                    nid_end += 1
                try:
                    net_id = window[nid_start:nid_end].strip().decode('utf-8')
                except:
                    continue
                
                # Find champion
                champ_pos = window.find(P_CHAMP_PRE)
                if champ_pos != -1:
                    champ_start = champ_pos + len(P_CHAMP_PRE)
                    champ_end = window.find(b'"', champ_start)
                    if champ_end != -1:
                        try:
                            champ = window[champ_start:champ_end].decode('utf-8')
                            stats['hero_net_ids'][net_id] = champ
                        except:
                            pass

def finalize_stats(stats, filename, patch, game_idx):
    """Convert stats to output row."""
    champs_str = "|".join(sorted(stats['champs']))
    hero_mapping = "|".join(f"{nid}:{name}" for nid, name in sorted(stats['hero_net_ids'].items()))
    
    return {
        'file': filename,
        'game_idx': game_idx,
        'patch': patch,
        'size_mb': round(stats['size_bytes'] / (1024 * 1024), 2),
        'duration_sec': round(stats['max_time'], 1),
        'duration_min': round(stats['max_time'] / 60, 2),
        'deaths': stats['deaths'],
        'spells': stats['spells'],
        'attacks': stats['attacks'],
        'damage': stats['damage'],
        'items': stats['items'],
        'n_champs': len(stats['champs']),
        'champs': champs_str,
        'hero_net_ids': hero_mapping,
        'n_heroes': len(stats['hero_net_ids']),
    }

def process_file_per_game(file_path):
    filename = os.path.basename(file_path)
    
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
    CHUNK_SIZE = 8 * 1024 * 1024

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
        keys = ['file', 'game_idx', 'patch', 'size_mb', 'duration_sec', 'duration_min',
                'deaths', 'spells', 'attacks', 'damage', 'items', 'n_champs', 'champs',
                'hero_net_ids', 'n_heroes']
        with open("metadata_per_game.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(all_rows)
        print(f"\n✅ Saved {len(all_rows)} games to metadata_per_game.csv")

if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()