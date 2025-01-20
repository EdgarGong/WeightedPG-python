import numpy as np
import gym
from gym import spaces

class CephBalancerEnv(gym.Env):
    def __init__(self, num_osds=10, num_pgs=100):
        super(CephBalancerEnv, self).__init__()
        self.num_osds = num_osds
        self.num_pgs = num_pgs
        
        # Observation space: OSD usage rates + PG weights
        self.observation_space = spaces.Box(low=0, high=1, shape=(num_osds + num_pgs,), dtype=np.float32)
        
        # Action space: PG weight adjustments (continuous values)
        self.action_space = spaces.Box(low=-0.1, high=0.1, shape=(num_pgs,), dtype=np.float32)
        
        # Initialize OSD and PG load states
        self.osd_usage = np.random.rand(num_osds)  # Initial OSD usage rates
        self.pg_weights = np.ones(num_pgs) / num_pgs  # Initial uniform PG weight distribution
        
    def step(self, action):
        # Update PG weights
        self.pg_weights = np.clip(self.pg_weights + action, 0, 1)
        self.pg_weights /= np.sum(self.pg_weights)  # Normalize weights
        
        # Update OSD usage rates based on the weights
        self.osd_usage = np.random.rand(self.num_osds)  # Simulate redistribution of PGs to OSDs
        
        # Reward: Evaluate the balance of OSD usage
        reward = -np.std(self.osd_usage)
        
        # Determine if the episode is done
        done = False  # Custom termination conditions can be added
        state = np.concatenate([self.osd_usage, self.pg_weights])
        return state, reward, done, {}
    
    def reset(self):
        # Reset OSD usage rates and PG weights
        self.osd_usage = np.random.rand(self.num_osds)
        self.pg_weights = np.ones(self.num_pgs) / self.num_pgs
        return np.concatenate([self.osd_usage, self.pg_weights])

from stable_baselines3 import PPO

# Create the environment
env = CephBalancerEnv(num_osds=10, num_pgs=100)

# Use the PPO algorithm
model = PPO("MlpPolicy", env, verbose=1)

# Train the model
model.learn(total_timesteps=100000)

# # Save the trained model
# model.save("ceph_balancer_ppo")


# # Load the trained model
# model = PPO.load("ceph_balancer_ppo")

# Evaluate the model
obs = env.reset()
for _ in range(100):
    action, _states = model.predict(obs)
    obs, reward, done, info = env.step(action)
    print("Reward: {reward}")
