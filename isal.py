import glob
import os
import csv
import multiprocessing
import time
from concurrent.futures import ProcessPoolExecutor, as_completed

# --- COMPRESSION LIBRARY SELECTOR ---
try:
    from isal import igzip as gzip
    print("🚀 Using: ISAL (Fastest)")
except ImportError:
    try:
        from indexed_gzip import IndexedGzipFile
        import gzip as std_gzip
        print("🚀 Using: Indexed_Gzip (Fast)")
        
        # Wrapper to make indexed_gzip look like standard gzip for our usage
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

def process_file_optimized(file_path):
    filename = os.path.basename(file_path)
    
    patch = "unknown"
    parts = file_path.replace("\\", "/").split("/")
    for p in parts:
        if p.startswith(("12_", "13_", "14_")):
            patch = p
            break

    stats = {
        'file': filename, 'patch': patch, 'games_est': 0,
        'deaths': 0, 'spells': 0, 'attacks': 0, 
        'damage': 0, 'items': 0, 'champs': set(),
        'size_mb': 0
    }

    # Byte patterns
    P_GAME_START = b'{"events":'
    P_DEATH = b'"HeroDie"'
    P_SPELL = b'"CastSpellAns"'
    P_ATTACK = b'"BasicAttackPos"'
    P_DAMAGE = b'"UnitApplyDamage"'
    P_ITEM = b'"BuyItem"'
    P_CHAMP_PRE = b'"champion":"'
    
    CHUNK_SIZE = 4 * 1024 * 1024 
    total_bytes = 0

    try:
        with gzip.open(file_path, 'rb') as f:
            while True:
                chunk = f.read(CHUNK_SIZE)
                if not chunk:
                    break
                
                total_bytes += len(chunk)
                
                stats['games_est'] += chunk.count(P_GAME_START)
                stats['deaths'] += chunk.count(P_DEATH)
                stats['spells'] += chunk.count(P_SPELL)
                stats['attacks'] += chunk.count(P_ATTACK)
                stats['damage'] += chunk.count(P_DAMAGE)
                stats['items'] += chunk.count(P_ITEM)
                
                if P_CHAMP_PRE in chunk:
                    parts = chunk.split(P_CHAMP_PRE)
                    for p in parts[1:]:
                        end = p.find(b'"')
                        if end != -1 and end < 30:
                            try:
                                stats['champs'].add(p[:end].decode('utf-8'))
                            except: pass

    except Exception as e:
        return None, f"Error {filename}: {e}"
    
    stats['size_mb'] = round(total_bytes / (1024*1024), 1)
    stats['champs'] = "|".join(sorted(list(stats['champs'])))
    stats['n_champs'] = len(stats['champs'].split("|")) if stats['champs'] else 0
    
    return [stats], None

def main():
    pattern = "D:/huggingface/maknee/12_22/*.jsonl.gz"
    files = sorted(glob.glob(pattern))
    print(f"Found {len(files)} files.")
    
    all_rows = []
    
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_file_optimized, f): f for f in files}
        
        if HAS_TQDM:
            pbar = tqdm(total=len(files), unit="file")
        
        for future in as_completed(futures):
            data, err = future.result()
            if data: all_rows.extend(data)
            elif err: print(f"\n{err}")
            
            if HAS_TQDM: pbar.update(1)

    if all_rows:
        keys = ['file', 'patch', 'size_mb', 'games_est', 'deaths', 'spells', 'attacks', 'damage', 'items', 'n_champs', 'champs']
        with open("metadata_optimized.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(all_rows)
        print("\nSaved to metadata_optimized.csv")

if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()