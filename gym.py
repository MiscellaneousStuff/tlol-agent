
import league_of_legends_decoded_replay_packets_gym as lol_gym

# Load professional replay data from HuggingFace
dataset = lol_gym.ReplayDataset([
    "12_22/"  # Download entire patch directory
], repo_id="maknee/league-of-legends-decoded-replay-packets")

# Or specific files
dataset = lol_gym.ReplayDataset([
    "12_22/batch_001.jsonl.gz",  # Professional matches from patch 12.22
    "12_22/batch_002.jsonl.gz"
], repo_id="maknee/league-of-legends-decoded-replay-packets")

dataset.load(max_games=10)  # Load first 10 games

# Create Gymnasium environment
env = lol_gym.LeagueReplaysEnv(dataset, time_step=1.0)
obs, info = env.reset()

print(f"🎮 Loaded game {info['game_id']}")
print(f"⏰ Starting at time: {info['current_time']:.1f}s")

# Step through decoded replay packets
for step in range(100):
    print("step:", step)
    obs, reward, terminated, truncated, info = env.step(0)  # Continue action
    
    game_state = info['game_state']
    
    if game_state.heroes:
        print(f"Step {step}: t={game_state.current_time:.1f}s, "
              f"heroes={len(game_state.heroes)}, "
              f"events={len(game_state.events)}")
        
        # Access decoded packet data
        for net_id, hero in list(game_state.heroes.items())[:3]:
            pos = game_state.get_position(net_id)
            if pos:
                print(f"  {hero.get('name', 'Hero')}: ({pos.x:.0f}, {pos.z:.0f})")
    
    if terminated or truncated:
        print("🏁 Game ended, resetting...")
        obs, info = env.reset()

env.close()