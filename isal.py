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

def new_game_stats():
    return {
        'deaths': 0, 'spells': 0, 'attacks': 0,
        'damage': 0, 'items': 0, 'champs': set(), 'size_bytes': 0
    }

def process_game_chunk(chunk, stats):
    """Process a chunk of data for one game, updating stats in place."""
    stats['size_bytes'] += len(chunk)
    stats['deaths'] += chunk.count(P_DEATH)
    stats['spells'] += chunk.count(P_SPELL)
    stats['attacks'] += chunk.count(P_ATTACK)
    stats['damage'] += chunk.count(P_DAMAGE)
    stats['items'] += chunk.count(P_ITEM)
    
    if P_CHAMP_PRE in chunk:
        parts = chunk.split(P_CHAMP_PRE)
        for p in parts[1:]:
            end = p.find(b'"')
            if 0 < end < 30:
                try:
                    stats['champs'].add(p[:end].decode('utf-8'))
                except:
                    pass

def finalize_stats(stats, filename, patch, game_idx):
    """Convert stats to output row."""
    champs_str = "|".join(sorted(stats['champs']))
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
        'champs': champs_str
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
                
                # Prepend leftover buffer
                data = buffer + chunk
                
                # Split by newlines (JSONL = one JSON per line)
                lines = data.split(b'\n')
                
                # Last element might be incomplete - save for next iteration
                buffer = lines[-1]
                complete_lines = lines[:-1]
                
                for line in complete_lines:
                    if not line.strip():
                        continue
                    
                    # Process this complete game line
                    process_game_chunk(line, current_stats)
                    
                    # Finalize and save
                    results.append(finalize_stats(current_stats, filename, patch, game_idx))
                    game_idx += 1
                    current_stats = new_game_stats()
            
            # Handle final buffer (last game if no trailing newline)
            if buffer.strip():
                process_game_chunk(buffer, current_stats)
                results.append(finalize_stats(current_stats, filename, patch, game_idx))

    except Exception as e:
        return None, f"Error {filename}: {e}"
    
    return results, None

def main():
    pattern = "D:/huggingface/maknee/12_22/*.jsonl.gz"
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
        keys = ['file', 'game_idx', 'patch', 'size_mb', 'deaths', 'spells', 'attacks', 'damage', 'items', 'n_champs', 'champs']
        with open("metadata_per_game.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(all_rows)
        print(f"\n✅ Saved {len(all_rows)} games to metadata_per_game.csv")

if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()