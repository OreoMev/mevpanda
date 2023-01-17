# Mevpanda
How to run Mevpanda server

1. pip install -r backend/requirements.txt
2. Add your API keys and web3+beaconchain providers in server.py
3. Use waitress or gunicorn for production run or just run python server.py for testing

Tip: Specify the start slot and block at the start of server.py if you want just recent history or specific block to verify
