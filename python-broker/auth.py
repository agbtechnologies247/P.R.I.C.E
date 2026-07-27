import os
from fastapi import APIRouter, HTTPException
from fyers_apiv3 import fyersModel
import state
import websocket_handler
from models import TokenRequest

router = APIRouter()

@router.get("/auth_url")
def get_auth_url():
    client_id = os.environ.get("FYERS_CLIENT_ID")
    secret_key = os.environ.get("FYERS_SECRET_KEY")
    redirect_uri = os.environ.get("FYERS_REDIRECT_URI", "https://price.agbtechnologies.in")
    
    if not client_id or not secret_key:
        raise HTTPException(status_code=400, detail="FYERS_CLIENT_ID and FYERS_SECRET_KEY are not configured.")
        
    try:
        session = fyersModel.SessionModel(
            client_id=client_id,
            secret_key=secret_key,
            redirect_uri=redirect_uri,
            response_type="code",
            grant_type="authorization_code"
        )
        url = session.generate_authcode()
        return {"status": "success", "auth_url": url}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating auth url: {e}")

@router.post("/login_token")
def login_token(req: TokenRequest):
    client_id = os.environ.get("FYERS_CLIENT_ID")
    secret_key = os.environ.get("FYERS_SECRET_KEY")
    redirect_uri = os.environ.get("FYERS_REDIRECT_URI", "https://price.agbtechnologies.in")
    
    if not client_id or not secret_key:
        raise HTTPException(status_code=400, detail="FYERS_CLIENT_ID and FYERS_SECRET_KEY are not configured.")
        
    try:
        session = fyersModel.SessionModel(
            client_id=client_id,
            secret_key=secret_key,
            redirect_uri=redirect_uri,
            response_type="code",
            grant_type="authorization_code"
        )
        session.set_token(req.auth_code)
        res = session.generate_token()
        if res.get("s") == "ok":
            access_token = res.get("access_token")
            state.fyers_client = fyersModel.FyersModel(
                client_id=client_id,
                token=access_token,
                log_path=os.getcwd()
            )
            
            # Save token to token.txt so it persists across runs
            token_file_path = os.path.join(os.getcwd(), "token.txt")
            with open(token_file_path, "w") as f:
                f.write(access_token)
                
            # Start websocket client
            websocket_handler.start_fyers_socket(client_id, access_token)
            return {"status": "success", "access_token": access_token}
        else:
            raise HTTPException(status_code=400, detail=res.get("message", "Token generation failed"))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fyers exchange token error: {e}")

@router.get("/health")
def health():
    return {
        "status": "healthy" if state.fyers_client else "unconfigured", 
        "mode": "fyers",
        "initialized": state.fyers_client is not None
    }

@router.post("/login")
def login():
    state.get_client()
    return {"status": "success", "access_token": "fyers_active_session"}

@router.post("/logout")
def logout():
    token_file_path = os.path.join(os.getcwd(), "token.txt")
    if os.path.exists(token_file_path):
        try:
            os.remove(token_file_path)
        except Exception:
            pass
    state.fyers_client = None
    return {"status": "success"}

@router.get("/profile")
def profile():
    fyers = state.get_client()
    if fyers == "mock":
        return {
            "status": "success",
            "data": {
                "name": "Simulated Trader",
                "fy_id": "MOCK_FYERS",
                "email": "trader@mock.com",
                "pin_set": True
            }
        }
    try:
        res = fyers.get_profile()
        if res.get("s") == "ok":
            data = res.get("data", {})
            return {
                "status": "success",
                "data": {
                    "name": data.get("name", "Fyers Client"),
                    "fy_id": data.get("fy_id", "FYERS"),
                    "email": data.get("email_id", "fyers@client.com"),
                    "pin_set": True
                }
            }
        else:
            raise HTTPException(status_code=400, detail=res.get("message", "Fyers profile check failed"))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fyers client exception: {e}")
