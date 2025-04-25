import pandas as pd
import psycopg2
print('hola mundo')
conn = psycopg2.connect(database = "bd2_2s24", 
                        user = "admin", 
                        host= 'localhost',
                        password = "root1234",
                        port = 5432)

# dateframeP = pd.read_csv("./csv/player.csv", sep=',' )
# dateframeDCS = pd.read_csv("./csv/draft_combine_stats.csv", sep=',' )
# dataframeCPI = pd.read_csv("./csv/common_player_info.csv", sep=',' )
# dataframeIP = pd.read_csv("./csv/inactive_players.csv", sep=',' )
# dataframeDH = pd.read_csv("./csv/draft_history.csv", sep=',' )

# dateframeDCS = dateframeDCS.rename(columns={'player_id': 'id'})
# dataframeCPI = dataframeCPI.rename(columns={'person_id': 'id'})
# dataframeIP = dataframeIP.rename(columns={'player_id': 'id'})
# dataframeDH = dataframeDH.rename(columns={'person_id': 'id'})

# dataframeDH [['first_name', 'last_name']] = dataframeDH['player_name'].str.split(' ', n=1, expand=True)

# dateframe_player = pd.concat([
#     dateframeP[['id','first_name','last_name']],
#     dateframeDCS[['id','first_name','last_name']] 
#     ],ignore_index=True)

# # dateframe_player = dateframe_player.drop_duplicates()

# # dateframe_player = pd.concat([
# #     dateframe_player,
# #     dataframeCPI[['id','first_name','last_name']] 
# #     ],ignore_index=True)

# # dateframe_player = pd.concat([
# #     dateframe_player,
# #     dataframeIP[['id','first_name','last_name']] 
# #     ],ignore_index=True)

# # dateframe_player = pd.concat([
# #     dateframe_player,
# #     dataframeDH[['id','first_name','last_name']] 
# #     ],ignore_index=True)

# # dataframeDH.drop(columns=['first_name', 'last_name'])


# dateframe_player = dateframe_player.drop_duplicates()

# print(dateframe_player.sort_values(by=['id']))