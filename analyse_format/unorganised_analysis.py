import orjson

with open('BR1-2525639956-6-7-2022.json', 'rb') as f:
    data = orjson.loads(f.read())  # 3-10x faster than stdlib

def describe(obj, depth=0, max_depth=3):
    indent = "  " * depth
    if depth > max_depth:
        print(f"{indent}...")
        return
    if isinstance(obj, dict):
        print(f"{indent}dict with {len(obj)} keys:")
        for key in list(obj.keys())[:10]:  # first 10 keys
            print(f"{indent}  '{key}': ", end="")
            describe(obj[key], depth + 2, max_depth)
    elif isinstance(obj, list):
        print(f"list with {len(obj)} items")
        if obj:
            print(f"{indent}  [0]: ", end="")
            describe(obj[0], depth + 2, max_depth)
    else:
        print(f"{type(obj).__name__}: {str(obj)[:50]}")

describe(data)

import orjson

with open('BR1-2525639956-6-7-2022.json', 'rb') as f:
    data = orjson.loads(f.read())

def describe(obj, depth=0, max_depth=3):
    indent = "  " * depth
    if depth > max_depth:
        print(f"{indent}...")
        return
    if isinstance(obj, dict):
        print(f"{indent}dict with {len(obj)} keys:")
        for key in list(obj.keys())[:10]:
            print(f"{indent}  '{key}': ", end="")
            describe(obj[key], depth + 2, max_depth)
    elif isinstance(obj, list):
        print(f"list with {len(obj)} items")
        if obj:
            print(f"{indent}  [0]: ", end="")
            describe(obj[0], depth + 2, max_depth)
    else:
        print(f"{type(obj).__name__}: {str(obj)[:50]}")

describe(data)

# --- Additional exploration ---
print("\n" + "="*50)
print("GAME INFO")
print("="*50)

game = data['game_info']

print(f"Game ID: {game['gameid']}")
print(f"Duration: {game['game_duration_str']}")
print(f"Platform: {game['platform']}")
print(f"Date: {game['date']}")

# Explore packets
packets = [p for p in data['packets'] if p]
print(f"\n--- PACKETS ---")
print(f"Total packets: {len(packets)}")
print(f"First packet keys: {list(packets[0][0].keys())}")
print(f"\nFirst packet preview:")
print(orjson.dumps(packets[0], option=orjson.OPT_INDENT_2).decode()[:1000])

# Packet sizes
sizes = [len(orjson.dumps(p)) for p in packets]
print(f"\nPacket sizes:")
print(f"  Min: {min(sizes):,} bytes")
print(f"  Max: {max(sizes):,} bytes")
print(f"  Avg: {sum(sizes)//len(sizes):,} bytes")

count = {}
replications = []
for packet_group in packets:
    for packet in packet_group:
        packet_type = list(packet.keys())[0]
        if packet_type == "Replication": replications.append(packet)
        if not packet_type in count: count[packet_type] = 0
        count[packet_type] += 1
print(count)

print(replications[0])
print()
print(replications[1000])
print()
print(replications[10000])