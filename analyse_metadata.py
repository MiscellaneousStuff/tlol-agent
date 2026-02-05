import csv
from collections import Counter

champ_counts = Counter()
total_games = 0

with open("metadata_per_game.csv", "r", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        total_games += 1
        if row['champs']:
            for champ in row['champs'].split("|"):
                champ_counts[champ] += 1

print(f"{'Champion':<20} {'Games':>10} {'%':>8}")
print("-" * 40)
for champ, count in champ_counts.most_common():
    pct = (count / total_games) * 100
    print(f"{champ:<20} {count:>10,} {pct:>7.2f}%")

print(f"\nTotal games: {total_games:,}")
print(f"Total unique champions: {len(champ_counts)}")