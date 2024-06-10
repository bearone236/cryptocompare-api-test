import requests
from datetime import datetime
import pytz
import os
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from dotenv import load_dotenv
from flask import Flask, request

app = Flask(__name__)
load_dotenv()

SPREADSHEET_ID = os.getenv('SPREADSHEET_ID')

def get_latest_price(symbol):
    try:
        params = {"fsym": "BTC", "tsym": symbol, "limit": 2}
        response = requests.get("https://min-api.cryptocompare.com/data/histohour", params, timeout=10)
        response.raise_for_status()
        data = response.json()

        if data["Response"] == "Success" and len(data["Data"]) > 1:
            latest_data = data["Data"][-1]
            utc_time = datetime.fromtimestamp(latest_data["time"], pytz.utc)
            jst_time = utc_time.astimezone(pytz.timezone('Asia/Tokyo'))
            return {
                "date": jst_time.strftime('%Y-%m-%d'),
                "time": jst_time.strftime('%H:%M:%S'),
                "high": latest_data["high"],
                "low": latest_data["low"],
                "open": latest_data["open"],
                "close": latest_data["close"],
                "volumefrom": latest_data["volumefrom"],
                "volumeto": latest_data["volumeto"]
            }
        else:
            print(f"データが存在しません for {symbol}")
            return None
    except Exception as e:
        print(f"Error fetching price data for {symbol}: {e}")
        return None

def update_google_sheet(data, sheet_name):
    try:
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
        creds = None
        credentials_json = 'credentials.json'

        if os.path.exists('token.json'):
            creds = Credentials.from_authorized_user_file('token.json', SCOPES)
        
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(credentials_json, SCOPES)
                creds = flow.run_local_server(port=8080)

            with open('token.json', 'w') as token:
                token.write(creds.to_json())

        service = build('sheets', 'v4', credentials=creds)

        values = [[
            data['date'],
            data['time'],
            data['high'],
            data['low'],
            data['open'],
            data['close'],
            data['volumefrom'],
            data['volumeto']
        ]]

        sheet = service.spreadsheets().values().get(spreadsheetId=SPREADSHEET_ID, range=f'{sheet_name}!A:A').execute()
        existing_values = sheet.get('values', [])

        next_row = len(existing_values) + 1
        if next_row < 3:
            next_row = 3

        RANGE_NAME = f'{sheet_name}!A{next_row}'
        body = {
            'values': values
        }

        result = service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME,
            valueInputOption='RAW', body=body).execute()

        print(f'{result.get("updatedCells")} cells updated in sheet {sheet_name}.')
    except Exception as e:
        print(f"Error updating Google Sheet {sheet_name}: {e}")
        if 'invalid_grant' in str(e):
            print("Token has been expired or revoked. Please re-authenticate.")
            # 再認証が必要な場合の対処

@app.route('/', methods=['POST'])
def main(request):
    symbols = {"USDC": "USDC", "JPY": "JPY", "USD": "USD"}

    for sheet_name, symbol in symbols.items():
        data = get_latest_price(symbol)
        if data:
            update_google_sheet(data, sheet_name)
            print(f'Data updated to Google Sheets in sheet {sheet_name} successfully.')
        else:
            print(f'No data to update for {sheet_name}.')
    return "Completed", 200

if __name__ == "__main__":
    app.run(debug=True)
