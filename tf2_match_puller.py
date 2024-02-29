import requests
import pandas as pd
import numpy as np
import json
import time
from collections import defaultdict


def fetch_logs_data(title=None, map_name=None, uploader=None, player=None, limit=1000, offset=0):
    """
    Fetches logs data from the API and returns it in JSON format.

    Parameters:
    - title: Optional. Title text search (min. 2 characters).
    - map_name: Optional. Exact name of a map.
    - uploader: Optional. Uploader SteamID64.
    - player: Optional. One or more player SteamID64 values, comma-separated.
    - limit: Optional. Limit results (default 1000, maximum 10000).
    - offset: Optional. Offset results (default 0).

    Returns:
    - JSON list of logs.
    """
    api_url = 'http://logs.tf/api/v1/log'
    params = {
        'title': title,
        'map': map_name,
        'uploader': uploader,
        'player': player,
        'limit': limit,
        'offset': offset
    }
    # Filtering out None values
    params = {key: value for key, value in params.items() if value is not None}

    response = requests.get(api_url, params=params)

    # Check if the request was successful
    if response.status_code == 200:
        return response.json()  # Return the JSON response
    else:
        raise Exception(f"Failed to fetch data from API. Status code: {response.status_code}, Message: {response.text}")


def create_data_rows(match_data, MATCH_ID, map):
  match_info = []
  match_winner = "Draw"  # Default to "Draw" in case of a tie

  # Determine the match winner based on team scores
  red_score = match_data['teams']['Red']['caps']
  blue_score = match_data['teams']['Blue']['caps']
  if red_score > blue_score:
      match_winner = "Red"
  elif blue_score > red_score:
      match_winner = "Blue"

  for player_id, player_info in match_data['players'].items():
    for c in player_info['class_stats']:
      player_class = c['type']

    max_dmg = 0
    accuracy = 0  # Default value if no weapon meets criteria
    weapon_name = ""

    # Assuming we're only interested in the first class stats
    for weapon, stats in player_info['class_stats'][0]['weapon'].items():
        if stats['dmg'] > max_dmg:
            max_dmg = stats['dmg']
            weapon_name = weapon
            # Check if shots and hits are not zero for accuracy calculation
            if stats['shots'] != 0 and stats['hits'] != 0:
                accuracy = (stats['hits'] / stats['shots']) * 100  # Accuracy in percentage
            else:
                accuracy = None  # Reset accuracy if current max damage weapon doesn't meet criteria
      
    extracted_stats = {}
    extracted_stats['player_class'] = player_class
    extracted_stats['map'] = map
    extracted_stats['steam_id'] = player_id
    extracted_stats['match_id'] = MATCH_ID
    extracted_stats['match_time_seconds'] = match_data['length']
    extracted_stats['date'] = match_data['info']['date']
    extracted_stats['match_winner'] = match_winner
    extracted_stats['title'] = match_data['info']['title']

    # Iterate over all keys in the nested dictionary except 'class_stats'
    for stat_key, stat_value in player_info.items():
        if stat_key != "class_stats" and stat_key != 'medicstats' and stat_key != 'ubertypes':  # Skip 'class_stats' and 'medicstats'
            extracted_stats[stat_key] = stat_value
        if stat_key == 'medicstats':
          for med_key, med_value in player_info[stat_key].items():
            extracted_stats[med_key] = med_value
        if stat_key == 'ubertypes':
          for med_key, med_value in player_info[stat_key].items():
            extracted_stats[med_key] = med_value
    extracted_stats['weapon_accuracy'] = accuracy
    extracted_stats['weapon_name'] = weapon_name

    for steam_id in match_data['classkills']:
      if player_id == steam_id:
        for class_killed, amount in match_data['classkills'][steam_id].items():
          extracted_stats[class_killed + "_frags"] = amount

    match_info.append(extracted_stats)
  return match_info




final_df = None
maps = [
    'pl_upward_f11', 'koth_ashville_final', 'koth_product_final', 'koth_proplant_v8',
    'pl_vigil_rc10', 'cp_steel_f12', 'pl_swiftwater_final'
]
lim = 10000
MATCH_ID = 0
for map in maps:
    logs_data = fetch_logs_data(map_name=map, limit=lim)
    for log in logs_data['logs']:
        if MATCH_ID % 1000 == 0 and MATCH_ID != 0:
          pd.DataFrame.to_csv(final_df, f"tf2_match_data/tf2_stats_{MATCH_ID}_matches.csv")
        log_id = log['id']
        val = requests.get(f'http://logs.tf/json/{log_id}')
        if val.status_code == 200:
            try:
                match_data = val.json()
                player_data = create_data_rows(match_data, MATCH_ID, map)
                if len(player_data) == 18:
                    df = pd.DataFrame(player_data)
                    if final_df is None:
                        final_df = df
                    else:
                        final_df = pd.concat([final_df, df], axis=0, ignore_index=True)
                MATCH_ID += 1
            except requests.exceptions.JSONDecodeError:
                print(f"Failed to decode JSON for log_id: {log_id}")
        else:
            print(f"Request failed with status code {val.status_code} for log_id: {log_id}")

# Ensure final_df is not empty before attempting to display or manipulate it
if final_df is not None:
    print("created dataframe!")
    # It's safer to check if final_df is not empty before attempting to save it
    pd.DataFrame.to_csv(final_df, f"tf2_match_data/tf2_stats_{MATCH_ID}_matches.csv")
else:
    print("No data was concatenated.")




