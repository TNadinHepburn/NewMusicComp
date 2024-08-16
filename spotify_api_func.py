from auth_code import getToken
from tqdm import tqdm
import requests, csv, base64, json, pandas as pd, time, os, pickle, numpy as np, pickle

BASE_URL = 'https://api.spotify.com/v1'
TOKEN = getToken()
HEADERS = {"Authorization": f"Bearer {TOKEN}"}
COLUMNS = ["track_id","track_name","track_uri","duration","artist_id","artist_name","album_id","album_name","album_type","album_released","album_total_tracks"]

def songsPerArtist(df):
    artist_song_count = df.groupby(['artist_id', 'artist_name'])['track_id'].count().reset_index()
    artist_song_count.columns = ['artist_id', 'artist_name', 'song_count']

    # Save the artist_song_count DataFrame to a CSV file
    artist_song_count.to_csv('artist_song_count.csv', index=False)

def addToPlaylist(playlist_id,track_uris):
    # Ensure track_ids is always a list
    if isinstance(track_uris, str):
        track_uris = [track_uris]
    for i in tqdm(range(0, len(track_uris),100)):
        track_chunk = track_uris[i:i+100] #playlists/{playlist_id}/tracks
        url = f"{BASE_URL}/playlists/{playlist_id}/tracks?uris={','.join(track_chunk)}"
        response = requests.post(url, headers=HEADERS)

def empty_spotify_playlist(playlist_id):
    # Step 2: Retrieve all the playlist's tracks
    track_uris = []
    
    url = f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks?limit=50"
    
    while url:
        response = requests.get(url, headers=HEADERS)
        
        if response.status_code == 200:
            tracks = response.json()["items"]
            track_uris += [track["track"]["uri"] for track in tracks]
            url = response.json()["next"]
        else:
            print("Failed to retrieve the playlist's tracks.")
            return False
    
    # Step 4: Remove the tracks (in chunks of 100)
    chunked_uris = [track_uris[i:i+100] for i in range(0, len(track_uris), 100)]
    
    for chunk in tqdm(chunked_uris):
        response = requests.delete(
            f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks",
            headers=HEADERS,
            json={"tracks": [{"uri": uri} for uri in chunk]}
        )
    
        if response.status_code != 200:
            print("Failed to empty chunk.")
    
    print("Playlist emptied successfully.")
    return True

def updateUniqueArtists(playlistID, filename):
    # Try to load the file with artist ids if it doesnt exzist create empty list
    try:
        with open(filename, 'rb') as f:
            loaded_artists = pickle.load(f)
    except:
        loaded_artists = []
    # Get unique Artist IDs from a playlist
    unique_from_playlist = getUniqueArtistIDsFromPlaylist(playlistID)
    # Combine loaded and new IDs into one unique list
    updated_artists = list(set(loaded_artists) | set(unique_from_playlist))
    # Save updated list of unique artist ids to the pickle file
    with open(filename, 'wb') as f:
        pickle.dump(updated_artists,f)

def getLikedSongs():
    url = f"{BASE_URL}/me/tracks?limit=50"
    tracks = []
    progress_bar = tqdm(total=float('inf'), unit='iteration', ncols=80)
    while url:
        response = requests.get(url, headers=HEADERS)
        while response.status_code == 429:
            time.sleep(10)
            response = requests.get(url, headers=HEADERS)
        if response.status_code == 200:
            url = response.json().get("next")
            playlist_tracks = response.json()
            chunk_info = []
            for item in playlist_tracks["items"]:
                track_id = item["track"]["uri"]
                album_name = item["track"]["album"]["name"]
                album_release_date_acc = item["track"]["album"]["release_date_precision"]
                album_release = item["track"]["album"]["release_date"]
                if album_release_date_acc == "day":
                    album_release_date = album_release
                elif album_release_date_acc == "month":
                    album_release_date = album_release + "-01"
                elif album_release_date_acc == "year":
                    album_release_date = album_release + "-01-01"
                chunk_info.append({
                    "uri" : track_id,
                    "released" : album_release_date,
                    'album_name' : album_name,
                })
            tracks.extend(chunk_info)
            url = playlist_tracks["next"]
        progress_bar.update(1)
    df = pd.DataFrame(tracks,columns=['uri','album_name','released'])
    return df


def getUniqueArtistIDsFromPlaylist(playlist_id):
    artist_ids = set()
    url = f"{BASE_URL}/playlists/{playlist_id}/tracks?limit=100&fields=next,items.track.artists(id)"
    while url:
        response = requests.get(url, headers=HEADERS)
        while response.status_code == 429:
            time.sleep(10)
            response = requests.get(url, headers=HEADERS)
        if response.status_code == 200:
            playlist_tracks = response.json()
            for item in playlist_tracks["items"]:
                if item["track"] != None:
                    for artist in item["track"]["artists"]:
                        artist_ids.add(artist["id"])
            url = playlist_tracks["next"]
        else:
            print(f"FAILed {response.status_code}")
            break
    return list(artist_ids)

# Returns array containing album IDs
def getArtistAlbumIds(artist_id,album_types = 'album,single'):
    album_ids = []
    url = f"{BASE_URL}/artists/{artist_id}/albums?limit=50&include_groups={album_types}"
    while url:
        # Make the GET request
        response = requests.get(url, headers=HEADERS)
        while response.status_code == 429:
            time.sleep(10)
            print(response.headers)
            response = requests.get(url, headers=HEADERS)
            print(response.text)

        # Check if the request was successful
        if response.status_code == 200:
            # Extract the album id's from the response
            albums = response.json()["items"]
            album_ids.extend([album["id"] for album in albums])

            # Check if there is a next page of results
            url = response.json().get("next")
        else:
            # Print the error message if the request was unsuccessful
            print(f"Failed to get album id's: {response.text}")
            break
    return album_ids

def getTracksFromAlbums(album_ids):
    # Ensure album_ids is always a list
    if isinstance(album_ids, str):
        album_ids = [album_ids]

    tracks = []
    for i in range(0, len(album_ids),20):
        albums_chunk = album_ids[i:i+20]
        url = f"{BASE_URL}/albums/?ids={','.join(albums_chunk)}"
        response = requests.get(url, headers=HEADERS)
        while response.status_code == 429:
            time.sleep(10)
            print(response.headers)
            response = requests.get(url, headers=HEADERS)
            print(response.text)
        
        # Check if the request was successful
        if response.status_code == 200:
            # Extract the album and track information from the response
            album_infos = response.json()["albums"]
            for album_info in album_infos:
                album_id = album_info["id"]
                album_name = album_info["name"]
                album_type = album_info["album_type"]
                album_release_date_acc = album_info["release_date_precision"]
                if album_release_date_acc == "day":
                    album_release_date = album_info["release_date"]
                elif album_release_date_acc == "month":
                    album_release_date = album_info["release_date"] + "-01"
                elif album_release_date_acc == "year":
                    album_release_date = album_info["release_date"] + "-01-01"
                album_total_tracks = album_info["total_tracks"]
                album_tracks = album_info["tracks"]["items"]
                # Check if there are more pages of tracks
                while album_info["tracks"]["next"]:
                    # Make a GET request to the next page of tracks
                    next_page = requests.get(album_info["tracks"]["next"], headers=HEADERS)
                    while next_page.status_code == 429:
                        time.sleep(10)
                        print(response.headers)
                        next_page = requests.get(album_info["tracks"]["next"], headers=HEADERS)
                        print(next_page.text)

                    if next_page.status_code == 200:
                        album_info = {"tracks":next_page.json()}
                        album_tracks.extend(album_info["tracks"]["items"])
                    else:
                        print(f"Failed to get tracks from {album_id}: {response.text}")

                # Add the track information to the results
                for album_track in album_tracks:
                    tracks.append({
                        "track_id": album_track["id"],
                        "track_name": album_track["name"],
                        "track_uri": album_track["uri"],
                        "duration": album_track["duration_ms"],
                        "artist_id": album_track["artists"][0]["id"],
                        "artist_name": album_track["artists"][0]["name"],
                        "album_id": album_id,
                        "album_name": album_name,
                        "album_type": album_type,
                        "album_released": album_release_date,
                        "album_total_tracks": album_total_tracks,
                    })
        else:
            print(f"Failed to get album {album_id} : {response.text}")
    return tracks

def getAllTracksFromArtistID(artist_id):
    try:
        album_ids = getArtistAlbumIds(artist_id)
        all_album_tracks_info = getTracksFromAlbums(album_ids)
        df = pd.DataFrame(all_album_tracks_info,columns=COLUMNS)
    except:
        df = pd.DataFrame(columns=COLUMNS)
    return df


def lookForDup(df):
    if df.empty:
        return df
    # Check for duplicates based on track_name, artist_name and duration or if track_id is present > 1
    sorted_data = df.sort_values(by=["album_type","album_total_tracks","album_released"], ascending=[True,False,False])
    sorted_data["duplicated"] = sorted_data.duplicated(subset="track_id", keep="first") | ( sorted_data.duplicated(subset=["track_name", "artist_name", "duration"], keep="first") & (sorted_data.duplicated(subset="duration", keep="first") | sorted_data["duration"].abs().between(sorted_data["duration"].abs() - 2000, sorted_data["duration"].abs() + 2000)))

    sorted_data = sorted_data.query("duplicated != True")

    sorted_data = sorted_data.drop("duplicated", axis=1)
    return sorted_data

def getInstrumental(df):
    instrumentalness = []
    track_ids = df['track_id'].to_list()

    for i in tqdm(range(0,len(track_ids), 100)):
        chunk = track_ids[i:i+100]
        response = requests.get(f"{BASE_URL}/audio-features", headers=HEADERS, params={'ids': ",".join(chunk), "response_type": "incomplete", "fields": "audio_features(instrumentalness)"})
        while response.status_code != 200:
            time.sleep(10)
            response = requests.get(f"{BASE_URL}/audio-features", headers=HEADERS, params={'ids': ",".join(chunk), "response_type": "incomplete", "fields": "audio_features(instrumentalness)"})
        forma= response.json()['audio_features']
        for tracks in forma:
            if tracks is not None:
                instrumentalness.append(tracks['instrumentalness'])
            else:
                instrumentalness.append(0)
    dfinstrumentalness = df.copy()
    dfinstrumentalness['inst'] = instrumentalness
    return dfinstrumentalness

## MAIN CODE #### MAIN CODE #### MAIN CODE #### MAIN CODE #### MAIN CODE #### MAIN CODE #### MAIN CODE ##

try:
    with open("artistIDs", "rb") as fp:   # Unpickling
        artists = pickle.load(fp)
except:
    artists = set()

print("Total Artists: ", len(artists))

#### UPDATE ARTISTS LIST #### 
# with open("artistIDsOld", "rb") as fp:   # Unpickling
#     artistsOLD = pickle.load(fp)

# afterartists = list(set(artistsOLD).union(set(artists)))

# artists_new = list()
# artists_new = list(set(artists_new).union(set(getUniqueArtistIDsFromPlaylist('6U2Y6FsgIqvI6B0BxAIIgl')))) #newartists
# # artists_new = list(set(artists_new).union(set(getUniqueArtistIDsFromPlaylist('57iG4WHU29hMWDi09kJdnW')))) #previousmonth 
# artists_new = list(set(artists_new).union(set(getUniqueArtistIDsFromPlaylist('1ri76d5x4OVe9Kf4cWDcnd')))) #KOREAN NEW RELEASES
# artists_new = list(set(artists_new).union(set(getUniqueArtistIDsFromPlaylist('37i9dQZF1DXe5W6diBL5N4')))) #New Music K-Pop
# artists_new = list(set(artists_new).union(set(getUniqueArtistIDsFromPlaylist('37i9dQZF1DX9IALXsyt8zk')))) #RADAR korea
# artists_new = list(set(artists_new).union(set(getUniqueArtistIDsFromPlaylist('37i9dQZF1DX7vZYLzFGQXc')))) #Fresh Finds Korea
# new_artist_ids = [artist_id for artist_id in artists_new if artist_id not in artists]
# new_artist_ids_TEMP = [artist_id for artist_id in artists_new if artist_id not in afterartists]
# start_time = time.time()
# newArtistsDF = pd.DataFrame(columns=COLUMNS)
# for artist in tqdm(new_artist_ids_TEMP):
#     if time.time()-start_time > 3400:
#         start_time = time.time()
#         TOKEN = getToken()
#         HEADERS = {"Authorization": f"Bearer {TOKEN}"}
#     new_df = getAllTracksFromArtistID(artist)
#     cleaned_new_df = lookForDup(new_df.copy())
#     # remove rows using boolean mask
#     mask = (cleaned_new_df['track_id'].isin(newArtistsDF['track_id'])) | ((cleaned_new_df['track_name'].isin(newArtistsDF['track_name'])) &
#             (cleaned_new_df['artist_id'].isin(newArtistsDF['artist_id'])) &
#             (abs(cleaned_new_df['duration'] - newArtistsDF['duration']) <= 2000))
#     cleaned_new_df = cleaned_new_df[~mask]
#     newArtistsDF = pd.concat([newArtistsDF,cleaned_new_df],ignore_index=True)
#     newArtistsDF = lookForDup(newArtistsDF.copy())
# newArtistsSongDF = newArtistsDF.sort_values(ignore_index=True,by=['artist_name','album_released','album_name']) 
# newArtistsSongDF = newArtistsSongDF.query('album_released <= "2024-05-31"')

# newArtistsSongDF.to_csv('new-artists-june.csv',index=False)

# # # # Add songs to playlist # # #
# uris = newArtistsSongDF['track_uri'].to_list()
# uris1 = uris
# playlist_id = '4NNtBluTROGuwvWHfkWOGP'
# addToPlaylist(playlist_id,uris1)
# # playlist_id = '2vRym858yQ86YXlGGdyjuq'
# # addToPlaylist(playlist_id,uris2)

# artists = list(set(artists).union(set(new_artist_ids)))
# print("New Total Artists: ",len(artists))
# with open("artistIDs", "wb") as fp:   #Pickling
#     pickle.dump(artists, fp)

#### UPDATE ARTIST LIST END ####


try:
    with open("blockedArtistIDs", "rb") as fp:   # Unpickling
        ignore_artist_id = pickle.load(fp)
except:
    ignore_artist_id = set()
print("Total Blocked Artists: ",len(ignore_artist_id))

#### UPDATE BLOCKED ARTISTS ####

# blocked_artists = list(set(ignore_artist_id).union(set(getUniqueArtistIDsFromPlaylist('2bjuJFpJJUZoZ5vVyX3Bnj'))))
# with open("blockedArtistIDs", 'wb') as fp:
#     pickle.dump(blocked_artists, fp)

#### UPDATE BLOCKED ARTISTS END ####

if os.path.exists('allSongs.parquet'):
    finalDF = pd.read_parquet('allSongs.parquet')
else:
    finalDF = pd.DataFrame(columns=COLUMNS)

# # # # # with open("artistIDs", "wb") as fp:   #Pickling
# # # # #     pickle.dump(artists, fp)

#### FETCH NEW SONGS FROM ALL ARTISTS ####

start_time = time.time()
#3250-4250 check
# for artist in tqdm(artists[8250:]):
#     if artist not in ignore_artist_id:
#         if time.time()-start_time > 3400:
#             start_time = time.time()
#             TOKEN = getToken()
#             HEADERS = {"Authorization": f"Bearer {TOKEN}"}
#         new_df = getAllTracksFromArtistID(artist)
#         cleaned_new_df = lookForDup(new_df.copy())
#         # remove rows using boolean mask
#         mask = (cleaned_new_df['track_id'].isin(finalDF['track_id'])) 
#         # | ((cleaned_new_df['track_name'].isin(finalDF['track_name'])) &
#         #         (cleaned_new_df['artist_id'].isin(finalDF['artist_id'])) &
#         #         (abs(cleaned_new_df['duration'] - finalDF['duration']) <= 2000))
#         cleaned_new_df = cleaned_new_df[~mask]
#         finalDF = pd.concat([finalDF,cleaned_new_df],ignore_index=True)
#         finalDF = lookForDup(finalDF.copy())
    
# Save to csv/parquet
finalDF.to_parquet('allSongs.parquet', index=False)
finalDF.to_csv('allSongs.csv',index=False)

newsongdf = finalDF.query('album_released >= "2024-06-01" & album_released <= "2024-06-30"')
newsongdf = newsongdf[~newsongdf['artist_id'].isin(ignore_artist_id)]
# newsongdf = getInstrumental(newsongdf)
newsongdf = newsongdf.sort_values(ignore_index=True,by=['album_released','album_name']) 
newsongdf.to_csv('playlist-jun24.csv',index=False)
# finalDF.to_parquet('testallthing.parquet', index=False)

#### FETCH NEW SONGS FROM ALL ARTISTS END ####

#### ADD NEW SONGS TO PLAYLIST ####
# filtered_df = newsongdf[~newsongdf['artist_id'].isin(ignore_artist_id)]
# uris = filtered_df['track_uri'].to_list()
# playlist_id = '3GEZoE8s8RABmaYhaO9PZW'
# addToPlaylist(playlist_id,uris)
#### ADD NEW SONGS TO PLAYLIST END ####

# # UPDATE SORTED MAINS  # # # #
# playlists=['5GUd2rXEDL8aD1je39qvLq','6EqTok1VDmMlzVqX4EmZSn','5Z2O70mVEoJzKhyCdKQmHO']
# print('emptying playlists')
# for playlist_id in playlists:
#     empty_spotify_playlist(playlist_id)
# print('collecting liked songs')
# liked_songs = getLikedSongs()
# print('collected liked songs')
# liked_songs = liked_songs.sort_values(ignore_index=True,by=['released','album_name'])
# uris = liked_songs['uri'].to_list()
# print('adding to playlist')
# chunks=[]
# for i in tqdm(range(0,len(uris),10000)):
#     print(i)
#     chunks.append(uris[i:i+10000])
# counter = 0
# for chunk in chunks:
#     addToPlaylist(playlists[counter],chunk)
#     counter+=1
#### UPDATE SORTED MAINS END ####


# inst live lofi acapella accapella remix mix 伴奏 MR orchestra 
# acoustic unplugged sleep 'ao/en vivo' piano sped slowed 
# reverb reimagined arranged arrangment medieval radio

