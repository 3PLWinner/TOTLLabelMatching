import requests
from dotenv import set_key, load_dotenv
import os

dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)

USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")
SYSTEM_ID = os.getenv("SYSTEM_ID")

LOGIN_URL = "https://wms.3plwinner.com/VeraCore/Public.Api/api/Login"

print("Debug info:")
print(f"USERNAME: '{USERNAME}'")
print(f"PASSWORD: {'*' * len(PASSWORD) if PASSWORD else 'None'}")
print(f"SYSTEM_ID: '{SYSTEM_ID}'")
print(f".env file path: {dotenv_path}")
print(f".env file exists: {os.path.exists(dotenv_path)}")

payload = {
    "userName": "vwslabels",
    "password": PASSWORD,
    "systemId": SYSTEM_ID
}

response = requests.post(LOGIN_URL, json=payload)

if response.status_code == 200:
    json_response = response.json()
    token = json_response.get("Token")
    print("Login successful!")
    print("Token", token)

    if token:
        set_key(dotenv_path, "W_TOKEN", token)
        print("Token saved to .env file as W_TOKEN.")
else:
    print("Login failed.")
    print("Status Code:", response.status_code)
    print("Response JSON:", response.text)



