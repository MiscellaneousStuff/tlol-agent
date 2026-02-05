import glob
import os
import csv
import multiprocessing
import time
from collections import Counter

# --- IMPORTS FOR SPEED ---
try:
    from isal import igzip as gzip
    COMPRESSION = "ISAL (Fastest)"
except ImportError:
    try:
        from indexed_gzip import IndexedGzipFile
        import gzip as std_gzip
        COMPRESSION = "Indexed_Gzip (Fast)"
        class FastGzip:
            def open(self, filename, mode='rb'):
                return IndexedGzipFile(filename, drop_handles=False)
        gzip = FastGzip()
    except ImportError:
        import gzip
        COMPRESSION = "Standard Gzip (Slow)"

try:
    import orjson
    def fast_parse(text_bytes): return orjson.loads(text_bytes)
    PARSER = "orjson"
except ImportError:
    import json
    def fast_parse(text_bytes): return json.loads(text_bytes)
    PARSER = "json"

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False

# --- CONFIG ---
MAX_WORKERS = max(4, multiprocessing.cpu_count() - 2)

def process_file_accurate(file_path):
    filename = os.path.basename(file_path)
    
    # Patch extraction
    patch = "unknown"
    parts = file_path.replace("\\", "/").split("/")
    for p in parts:
        if p.startswith(("12_", "13_", "14_")):
            patch = p
            break
            
    stats = {
        'file': filename, 'patch': patch,
        'games': 0, 'duration_sum': 0, 'deaths': 0, 
        'damage': 0, 'packets': 0, 'champs': set()
    }
    
    try:
        with gzip.open(file_path, 'rb') as f:
            for line in f:
                if not line.strip(): continue
                try:
                    game = fast_parse(line)
                    events = game.get('events', [])
                    
                    stats['games'] += 1
                    stats['packets'] += len(events)
                    max_time = 0.0
                    
                    for e in events:
                        for k, v in e.items():
                            if k == 'HeroDie': stats['deaths'] += 1
                            elif k == 'UnitApplyDamage': stats['damage'] += 1
                            elif k == 'CreateHero':
                                c = v.get('champion')
                                if c: stats['champs'].add(c)
                            
                            if isinstance(v, dict):
                                t = v.get('time')
                                if t and isinstance(t, (int, float)) and t > max_time:
                                    max_time = t
                    stats['duration_sum'] += max_time
                except: continue 

    except Exception as e:
        return None, f"Error {filename}: {e}"
    
    stats['avg_duration'] = round(stats['duration_sum'] / stats['games'], 1) if stats['games'] > 0 else 0
    stats['n_champs'] = len(stats['champs'])
    stats['champs'] = "|".join(sorted(list(stats['champs'])))
    del stats['duration_sum']
    
    return [stats], None

def main():
    pattern = "D:/huggingface/maknee/12_22/*.jsonl.gz"
    files = sorted(glob.glob(pattern))
    
    print(f"--- SETUP ---")
    print(f"Files:       {len(files)}")
    print(f"Reader:      {COMPRESSION}")
    print(f"Parser:      {PARSER}")
    print(f"Workers:     {MAX_WORKERS}")
    print(f"-------------")
    
    all_rows = []
    
    # The Pool
    with multiprocessing.Pool(processes=MAX_WORKERS, maxtasksperchild=1) as pool:
        
        # The Iterator
        iterator = pool.imap_unordered(process_file_accurate, files)
        
        # The Progress Bar
        if HAS_TQDM:
            # unit='file' makes it say "5.2 file/s"
            # smoothing=0.1 makes the ETA update faster
            iterator = tqdm(iterator, total=len(files), unit="file", smoothing=0.1)
        
        start_time = time.time()
        
        for result, err in iterator:
            if result:
                all_rows.extend(result)
            elif err and not HAS_TQDM:
                print(f"Error: {err}")

            # Manual fallback if TQDM is missing
            if not HAS_TQDM and len(all_rows) % 5 == 0:
                elapsed = time.time() - start_time
                speed = len(all_rows) / elapsed
                remaining = (len(files) - len(all_rows)) / speed
                print(f"Processed {len(all_rows)}/{len(files)} - {speed:.2f} file/s - ETA: {remaining/60:.1f}m", end='\r')

    # Save
    if all_rows:
        keys = ['file', 'patch', 'games', 'avg_duration', 'deaths', 'damage', 'packets', 'n_champs', 'champs']
        with open("metadata_accurate.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=keys, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(all_rows)
        print("\nSaved to metadata_accurate.csv")

if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()