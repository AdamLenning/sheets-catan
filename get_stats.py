from __future__ import print_function
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
import pandas as pd
from IPython.display import HTML

import logging
import boto3
import boto3.session
from botocore.exceptions import ClientError

#ELO Weights
WIN = 1
TIE = 0.5
LOSS = 0

class CatanStats():

    def __init__(self):        
        # If modifying these scopes, delete the file token.json.
        self.SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

        # The ID and range of a sample spreadsheet.
        self.SAMPLE_SPREADSHEET_ID = '1MG7jhiarGpRunEILVbkEfD3mLRUBpIMI4au7cMID_EM'
        self.SAMPLE_RANGE_NAME = 'Games'

        # Dataframe containing Games and All Player Data from Google Sheets
        self.games, self.player_scores = self.get_games()

        # Initialize Elo Dict
        self.elo = self.elo_init()

        for index, row in self.games.iterrows():
            self.new_ratings(row)

    # Fetches data from Google Sheets
    def get_games(self):
        creds = None
        # The file token.json stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        if os.path.exists('token.json'):
            creds = Credentials.from_authorized_user_file('token.json', self.SCOPES)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    'credentials.json', self.SCOPES)
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open('token.json', 'w') as token:
                token.write(creds.to_json())
                
        service = build('sheets', 'v4', credentials=creds)

        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=self.SAMPLE_SPREADSHEET_ID,
                                    range=self.SAMPLE_RANGE_NAME).execute()
        values = result.get('values', [])
        
        df = pd.DataFrame(values[1:], columns=values[0])

        p1 = df[['Player 1', 'Starting Production 1', 'Ending VP 1']].rename(columns={"Player 1": "Player", "Starting Production 1": "Starting Production",  "Ending VP 1": "Ending VP"})
        p2 = df[['Player 2', 'Starting Production 2', 'Ending VP 2']].rename(columns={"Player 2": "Player", "Starting Production 2": "Starting Production",  "Ending VP 2": "Ending VP"})
        p3 = df[['Player 3', 'Starting Production 3', 'Ending VP 3']].rename(columns={"Player 3": "Player", "Starting Production 3": "Starting Production",  "Ending VP 3": "Ending VP"})
        p4 = df[['Player 4', 'Starting Production 4', 'Ending VP 4']].rename(columns={"Player 4": "Player", "Starting Production 4": "Starting Production",  "Ending VP 4": "Ending VP"})

        players = pd.concat([p1, p2, p3, p4])
        
        nan_value = float("NaN")
        players.replace("", nan_value, inplace=True)
        players = players.dropna()

        players = players.reset_index()
        players_scores = players.apply(pd.to_numeric, errors='ignore')
        
        return df, players_scores

    # Initializes Elo Dict
    def elo_init(self):
        elo = {}
        for player in self.player_scores['Player'].unique():
            elo[player] = 1000
        return elo
    
    # Individual elo calculation based on outcome
    def elo_calc(self, eloA, eloB, actual_outcome):
        K_VAL = 15
        expected_outcome = 1 / (1 + 10**((eloB - eloA)/400) )
        elo_adjustment_a = K_VAL * (actual_outcome - expected_outcome)
        return elo_adjustment_a

    # Calculates new elo ratings for game
    def new_ratings(self, game):
        
        # Check if Variant Game
        if(game["Variant"] == "TRUE"):
            variant = True
        else:
            variant = False

        # Check if 3 or 4 player
        if(game["Player 4"] == ''):
            four_player_game = False
        else:
            four_player_game = True

        # Pairwise elo_calc
        player_elo_adj = []

        if(not variant):
            if(four_player_game):

                # Iterate over pairwise player matchups
                for i in range(1,5):
                    
                    player_cummulative = 0

                    for j in range(1,5):

                        if(int(game["Ending VP {}".format(i)]) > int(game["Ending VP {}".format(j)])):
                            outcome = WIN
                        elif(int(game["Ending VP {}".format(i)]) == int(game["Ending VP {}".format(j)])):
                            outcome = TIE
                        else:
                            outcome = LOSS
                        
                        match_up = self.elo_calc(self.elo[game['Player {}'.format(i)]], self.elo[game['Player {}'.format(j)]], outcome)

                        player_cummulative += match_up
                    
                    player_elo_adj.append(player_cummulative)

                # Elo adjustments after calculations
                for i in range(0,4):
                    self.elo[game['Player {}'.format(i + 1)]] += round(player_elo_adj[i])

            else:
                # Iterate over pairwise player matchups
                for i in range(1,4):
        
                    player_cummulative = 0

                    for j in range(1,4):
                        try:
                            if(int(game["Ending VP {}".format(i)]) > int(game["Ending VP {}".format(j)])):
                                outcome = WIN
                            elif(int(game["Ending VP {}".format(i)]) == int(game["Ending VP {}".format(j)])):
                                outcome = TIE
                            else:
                                outcome = LOSS
                        except ValueError:
                            print(game["Game #"],i, " ", j, " ", game["Ending VP {}".format(i)], " " , game["Ending VP {}".format(j)] )
                            
                        
                        match_up = self.elo_calc(self.elo[game['Player {}'.format(i)]], self.elo[game['Player {}'.format(j)]], outcome)
                        
                        player_cummulative += match_up
                    
                    player_elo_adj.append(player_cummulative)

                # Elo adjustments after calculations
                for i in range(0,3):
                    self.elo[game['Player {}'.format(i + 1)]] += round(player_elo_adj[i])

        return None

    # Aggregate Stats on Players
    def player_info(self):

        # Add Elo scores to Player_Scores
        self.player_scores['Elo'] = self.player_scores.apply(lambda row: int(self.elo[row['Player']]), axis = 1)

        # Add aggregate data to player_stats df
        player_stats = self.player_scores.groupby('Player').agg({'Starting Production': ['mean', 'std'], 'Ending VP': ['mean', 'std'], 'Elo': ['mean']})
        player_stats["Games Played"] = self.player_scores.groupby('Player').size()
        player_stats["Wins"] = self.player_scores.loc[self.player_scores['Ending VP'] >= 10].groupby('Player').size()
        player_stats["Win Percentage"] = player_stats["Wins"] / player_stats["Games Played"]        

        # Rounds data to 2 decimal spots
        player_stats[('Starting Production','mean')] = player_stats[('Starting Production','mean')].round(2)
        player_stats[('Starting Production','std')] = player_stats[('Starting Production','std')].round(2)
        player_stats[('Ending VP','mean')] = player_stats[('Ending VP','mean')].round(2)
        player_stats[('Ending VP','std')] = player_stats[('Ending VP','std')].round(2)
        player_stats['Win Percentage'] = (player_stats['Win Percentage'] * 100).round(2)

        # Filter players who have played more than 1 game
        player_stats = player_stats[player_stats['Games Played'] > 1]

        # return player_stats.sort_values([('Ending VP', 'mean'),('Elo', 'mean'),"Win Percentage"], ascending=False)
        # return player_stats.sort_values(["Games Played", ('Ending VP', 'mean'),('Elo', 'mean'),"Win Percentage"], ascending=False)
        # return player_stats.sort_values([('Starting Production', 'mean'),"Win Percentage",('Ending VP', 'mean')], ascending=False)
        # return player_stats.sort_values(["Win Percentage",('Elo', 'mean'),('Ending VP', 'mean')], ascending=False)
        return player_stats.sort_values([('Elo', 'mean'),"Win Percentage",('Ending VP', 'mean')], ascending=False)

    # Aggregate Stats on Dice
    def dice_info(self):
        df = get_data()

    # Creates HTML File from player_info
    def create_player_table(self):

        html = self.player_info().to_html(classes=["table", "table-striped"])

        # write html to file
        text_file = open("index.html", "w")
        text_file.write('<link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.3.1/css/bootstrap.min.css" integrity="sha384-ggOyR0iXCbMQv3Xipma34MD+dH/1fQ784/j6cY/iJTQUOhcWr7x9JvoRxT2MZw1T" crossorigin="anonymous">\n')
        text_file.write(html)
        text_file.close()

def upload_s3(file):

    session = boto3.session.Session(profile_name='default')
    s3_client = session.client('s3')

    try:
        s3_client.upload_file(Filename=file, Bucket="adamlenning.com", Key=file,  ExtraArgs={'ContentType': "text/html"})
    except ClientError as e:
        logging.error(e)
        return False
    return True

if __name__ == '__main__':
    c1 = CatanStats()
    c1.create_player_table()
    upload_s3('index.html')
