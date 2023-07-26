# Mobile_Game_Analysis

Increment the scores of Player and its Team, where 1 kill =  1 score

For every kill a player has taken with a particular weapon:
- if the battle time is between 10 and 20 seconds, 4 points
- if the battle time is between 21 and 30 seconds, 3 points
- if the battle time is between 31 and 40 seconds, 2 points
- if the battle time is >40, 1 point

if the difference between the weapon rank:
- is more thann 6 ranks, 3 points
- is between 3 annd 5 ranks, 2 points
- else, 1 point

if the killer's location is not equal to eliminated player, assing 3 points
