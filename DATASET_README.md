---
license: apache-2.0
task_categories:
- reinforcement-learning
- time-series-forecasting
- other
language:
- en
tags:
- gaming
- esports
- league-of-legends
- behavioral-data
- game-analytics
- time-series
- lol
- replays
size_categories:
- 10M<n<100M
dataset_info:
  splits:
  - name: 12_22
  - name: 12_23
  - name: 13_01
  - name: 13_02
  - name: 13_03
---

# Disclaimer

This work isn’t endorsed by Riot Games and doesn’t reflect the views or opinions of Riot Games or anyone officially involved in producing or managing League of Legends. League of Legends and Riot Games are trademarks or registered trademarks of Riot Games, Inc.

# League of Legends Replays Dataset

This dataset contains over **1TB+** (700k+ replays) of League of Legends game replay data for research in gaming analytics, behavioral modeling, and reinforcement learning applications. The dataset is organized by game patch versions (12_22, 12_23, etc.) with parsed packet-level game events. You can find [older unorganized replays here](https://huggingface.co/datasets/maknee/leaague-of-legends-decoded-replay-packets-s12-unorganized).

## Data Format

Each match JSONL file contains multiple maches of chronologically ordered list of packets representing all game events:

```json
{
  "events": [
    {
      "WaypointGroup": {
        "time": 1.2,
        "waypoints": {
          "1001": [{"x": 100.5, "z": 200.3}]
        }
      }
    },
    {
      "CastSpellAns": {
        "time": 10.23234,
        "champion_caster_id": 1073741859,
        "spell_name": "AkaliE",
        "level": 1,
        "source_position": {
          "x": 14045.15,
          "z": 13559.334
        },
        "target_ids": [],
        "windup_time": 0.25,
        "cooldown": 14.5,
        "mana_cost": 30.0,
        "slot": 2
      }
    },
    {
      "BasicAttackPos": {
        "time": 122.12,
        "source_id": 1073741859,
        "target_id": 1073741858,
        "source_position": {
          "x": 9222.389,
          "z": 2501.3594
        },
        "target_position": {
          "x": 9266.0,
          "z": 2522.0
        },
      }
    },
    {
      "ReplicationData": {
        "time": 721.11426,
        "1073741859": {
          [
            {
              "name": "health",
              "data": 1516.6107
            },
          ]
        }
      }
    },
  ]
}
```

## Usage

### Loading the Dataset

There's two ways to use this dataset:

**Option 1: Using the [Gym Environment](https://pypi.org/project/league-of-legends-decoded-replay-packets-gym/) (Recommended)**
```python
pip install league-of-legends-decoded-replay-packets-gym

import league_of_legends_decoded_replay_packets_gym as lol_gym

dataset = lol_gym.ReplayDataset([
    "12_22/batch_001.jsonl.gz",
], repo_id="maknee/league-of-legends-decoded-replay-packets")

dataset.load(max_games=1)
print(f"Match has {len(dataset[0])} packets")
```

**Option 2: Manual Download and Processing**

```python
from huggingface_hub import hf_hub_download
import json
import gzip

# Download and process directly
local_file = hf_hub_download(
    repo_id="maknee/league-of-legends-decoded-replay-packets",
    filename="12_22/batch_001.jsonl.gz",
    repo_type="dataset"
)

# Process compressed file directly
with gzip.open(local_file, 'rt', encoding='utf-8') as f:
    for line_num, line in enumerate(f):
          match_data = json.loads(line)
          packets = match_data["events"]
          print(f"Match {line_num+1} has {len(packets)} packets")
```

## Packet Schema

The dataset contains **20 packet types** capturing all game events:

| Packet Type | Description |
|-------------|-------------|
| `CreateHero` | Champion spawn and initialization |
| `HeroDie` | Champion death events |
| `WaypointGroup` | Movement commands and pathfinding |
| `WaypointGroupWithSpeed` | Movement commands with speed data |
| `EnterFog` | Entity entering fog of war |
| `LeaveFog` | Entity leaving fog of war |
| `UnitApplyDamage` | Damage dealt between units |
| `DoSetCooldown` | Ability cooldown updates |
| `BasicAttackPos` | Basic attack with positional data |
| `CastSpellAns` | Spell/ability casting events |
| `BarrackSpawnUnit` | Minion spawning from barracks |
| `SpawnMinion` | General minion spawn events |
| `CreateNeutral` | Neutral monster creation |
| `CreateTurret` | Turret/tower initialization |
| `NPCDieMapView` | NPC death (map view) |
| `NPCDieMapViewBroadcast` | NPC death broadcast |
| `BuyItem` | Item purchase events |
| `RemoveItem` | Item removal/selling |
| `SwapItem` | Item slot swapping |
| `UseItem` | Item activation/usage |
| `Replication` | Game state synchronization |

For complete packet definitions and Python dataclasses, see [`packets.py`](packets.py).

## Applications

This dataset enables research in several domains:

- Reinforcement Learning
- Game Analytics  
- Behavioral Research

## [Examples from gym](https://github.com/Maknee/league-of-legends-decoded-replay-packets-gym)

- [Champion position visualization](https://github.com/Maknee/league-of-legends-decoded-replay-packets-gym/blob/ad6f264912d7b0e733292f4f3413ab9b59eb7607/league_of_legends_decoded_replay_packets_gym/examples/champion_gif_generator.py)
![positions](https://raw.githubusercontent.com/Maknee/league-of-legends-decoded-replay-packets-gym/ad6f264912d7b0e733292f4f3413ab9b59eb7607/champion_movement.gif)

- [Training RL agent (OpenLeague5) similar to OpenAI Five](https://github.com/Maknee/league-of-legends-decoded-replay-packets-gym/tree/ad6f264912d7b0e733292f4f3413ab9b59eb7607/league_of_legends_decoded_replay_packets_gym/examples/openleague5)
```bash
🎯 Prediction Results:
==============================
Action: Use W Ability
Confidence: 0.354
State Value: -0.681
Target Position: (7266, 3750) world coords
Coordinate Confidence: X=0.158, Y=0.080
Unit Target: 0
Unit Confidence: 1.000
✅ Prediction completed successfully!
```

## Citation

If you use this dataset in your research, please cite:

```bibtex
@dataset{league_of_legends_decoded_replay_packets_2025,
  title={League of Legends Decoded Replay Packets Dataset},
  author={maknee},
  year={2025},
  url={https://huggingface.co/datasets/maknee/league-of-legends-decoded-replay-packets}
}
```

## License

This dataset is released under the Apache 2.0 License.