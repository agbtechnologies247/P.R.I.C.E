import os
from fastapi import HTTPException
from fyers_apiv3 import fyersModel

fyers_client = None
fyers_socket = None
main_loop = None
connected_ws = []

def get_client() -> fyersModel.FyersModel:
    global fyers_client
    if fyers_client is None:
        client_id = os.environ.get("FYERS_CLIENT_ID")
        token_file_path = os.path.join(os.getcwd(), "token.txt")
        if client_id and os.path.exists(token_file_path):
            try:
                with open(token_file_path, "r") as f:
                    token = f.read().strip()
                if token:
                    fyers_client = fyersModel.FyersModel(
                        client_id=client_id,
                        token=token,
                        log_path=os.getcwd()
                    )
                    print("Dynamically initialized Fyers client from token.txt")
            except Exception as e:
                print(f"Error dynamically initializing Fyers client: {e}")

    if fyers_client is None:
        raise HTTPException(
            status_code=503, 
            detail="Fyers API Client is not initialized. Ensure FYERS_CLIENT_ID and FYERS_ACCESS_TOKEN are set."
        )
    return fyers_client
