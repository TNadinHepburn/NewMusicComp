import requests, csv, base64, json, pandas as pd
from spotify_secrets import *
from tqdm import tqdm
# URLS
AUTH_URL = 'https://accounts.spotify.com/authorize'
TOKEN_URL = 'https://accounts.spotify.com/api/token'
BASE_URL = 'https://api.spotify.com/v1/'

def auth(id,secret):
    # Step 1 - Authorization 
    url = "https://accounts.spotify.com/api/token"
    headers = {}
    data = {}
    # Encode as Base64 
    message = f"{id}:{secret}"
    messageBytes = message.encode('ascii')
    base64Bytes = base64.b64encode(messageBytes)
    base64Message = base64Bytes.decode('ascii')
    headers['Authorization'] = f"Basic {base64Message}"
    data['grant_type'] = "client_credentials"
    r = requests.post(url, headers=headers, data=data)
    return r.json()['access_token']

def display_playlist_100(id,urlendpoint,offset):
    url = f"https://api.spotify.com/v1/playlists/{id}/tracks?offset={offset}"
    response = requests.get(
        url=url,
        headers={
            "Authorization" : "Bearer " + token,
        },
    )
    json_resp = response.json()
    return json_resp

def get_playlist_size(id):
    url = f"https://api.spotify.com/v1/playlists/{id}"
    response = requests.get(
        url=url,
        headers={
            "Authorization" : "Bearer " + token,
        },
    )
    json_resp = response.json()
    play_size=json_resp.get('tracks').get('total')
    return play_size   

def get_playlist_name(id):
    url = f"https://api.spotify.com/v1/playlists/{id}"
    response = requests.get(
        url=url,
        headers={
            "Authorization" : "Bearer " + token,
        },
    )
    json_resp = response.json()
    title=json_resp.get('name')
    return title    

def playlist_to_csv(id):
    f = csv.writer(open(f"./playlists/{get_playlist_name(id)}.csv", "w",newline='',encoding='utf-8'))
    for offset in tqdm(range(0,get_playlist_size(id),100)):
        playlist = display_playlist_100(id,offset)
        for song in playlist.get('items'):
            track = song.get('track')
            tname = track.get('name')
            talbum = track.get('album').get('name')
            tartist = track.get('artists')[0].get('name')
            f.writerow([tname,talbum,tartist])

def playlist_to_array(id):
    plist = []
    for offset in tqdm(range(0,get_playlist_size(id),100)):
        playlist = display_playlist_100(id,offset)
        for song in playlist.get('items'):
            track = song.get('track')
            tname = track.get('name')
            talbum = track.get('album').get('name')
            tartist = track.get('artists')[0].get('name')
            plist.append({'Name':tname,'Artist':tartist,'Album':talbum})
    return plist

def playlist_to_df(id):
    plist = []
    for offset in tqdm(range(0,get_playlist_size(id),100)):
        playlist = display_playlist_100(id,offset)
        for song in playlist.get('items'):
            track = song.get('track')
            tname = track.get('name')
            talbum = track.get('album').get('name')
            tartist = track.get('artists')[0].get('name')
            plist.append({'Name':tname,'Artist':tartist,'Album':talbum})
    return pd.DataFrame(plist)

def duplicates_2_playlists(id1,id2):
    playlist_id1 = playlist_to_df(id1)
    playlist_id2 = playlist_to_df(id2)
    duplicates = pd.merge(playlist_id1,playlist_id2,'inner',on=['Name','Artist'])
    print(duplicates)
    return duplicates

DICT_PLAYLIST_IDs = [{'Name':'car','id':'5HPch794ARWkfeElEcG6bf'},{'Name':'main 1','id':'05wx09rkSsppS0Sw3tlJ7z'},{'Name':'all music 1','id':'1FyZwWBSRJKHxV5bgigWwn'},{'Name':'all music 2','id':'3IaWGXZwaIHqJ8QnwrS6HQ'},{'Name':'all music 3','id':'3DEOa14spSGXYiMp6IKo4F'},{'Name':'need to listen','id':'4lTPj7W5UMlxf75gVWWJId'}]
# car, main 1, all music 1, all music 2, all music 3, need to listen  
PLAYLIST_IDs = ['5HPch794ARWkfeElEcG6bf','05wx09rkSsppS0Sw3tlJ7z','1FyZwWBSRJKHxV5bgigWwn','3IaWGXZwaIHqJ8QnwrS6HQ','3DEOa14spSGXYiMp6IKo4F','4lTPj7W5UMlxf75gVWWJId']





token = auth(clientId,clientSecret,clientRedirectURI)

playlistcheck = pd.concat([playlist_to_df('0bN8so2A8qtQNq0SJkqGYg'),playlist_to_df('2SuvNlTJudIlKsS4GZfK3h'),playlist_to_df('4b1wXQkUjiN6MbLO8qb0so'),playlist_to_df('6bTvPRjjtqgc2lEGG49ATN'),],ignore_index=True)
playlistcheck.to_csv(f'./playlists/playlist check (all).csv', encoding='UTF-8', index=False)
# dups = duplicates_2_playlists(PLAYLIST_IDs[1],PLAYLIST_IDs[5])
