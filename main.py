from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import RedirectResponse
from fyers_apiv3 import fyersModel
import os
from typing import Optional
import uvicorn

app = FastAPI(title="Fyers API Integration", version="1.0.0")

# Fyers configuration - ideally load from environment variables
FYERS_CLIENT_ID = os.getenv("FYERS_CLIENT_ID", "XCXXXXXxxM-100")
FYERS_SECRET_KEY = os.getenv("FYERS_SECRET_KEY", "MH*****TJ5")
FYERS_REDIRECT_URI = os.getenv("FYERS_REDIRECT_URI", "https://yourdomain.com/callback")

# Store tokens in memory (use database in production)
token_storage = {}

@app.get("/")
async def root():
    return {"message": "Fyers API Integration Service", "status": "running"}

@app.get("/login")
async def login_fyers():
    """
    Initiate Fyers login flow
    Returns redirect URL for user authentication
    """
    try:
        # Create session model
        appSession = fyersModel.SessionModel(
            client_id=FYERS_CLIENT_ID,
            redirect_uri=FYERS_REDIRECT_URI,
            response_type="code",
            state="sample_state",
            secret_key=FYERS_SECRET_KEY,
            grant_type="authorization_code"
        )

        # Generate authentication URL
        auth_url = appSession.generate_authcode()

        return {
            "status": "success",
            "message": "Please visit the URL to authenticate",
            "auth_url": auth_url,
            "next_step": f"After authentication, call /generate-token with the auth_code received at {FYERS_REDIRECT_URI}"
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Login initiation failed: {str(e)}")

@app.get("/callback")
async def auth_callback(
    code: str = Query(..., description="Authorization code from Fyers"),
    state: str = Query("sample_state", description="State parameter")
):
    """
    Callback endpoint for Fyers OAuth redirect
    This should match your registered redirect_uri
    """
    try:
        # Store the auth code temporarily
        token_storage["auth_code"] = code

        return {
            "status": "success",
            "message": "Authorization code received successfully",
            "auth_code": code,
            "next_step": "Call /generate-token to get access token"
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Callback processing failed: {str(e)}")

@app.get("/generate-token")
async def generate_token(auth_code: Optional[str] = None):
    """
    Generate access token using authorization code
    """
    try:
        # Use provided auth_code or stored one
        if auth_code is None:
            auth_code = token_storage.get("auth_code")

        if not auth_code:
            raise HTTPException(
                status_code=400,
                detail="No auth code provided. Please authenticate first via /login"
            )

        # Create session and generate token
        appSession = fyersModel.SessionModel(
            client_id=FYERS_CLIENT_ID,
            redirect_uri=FYERS_REDIRECT_URI,
            response_type="code",
            state="sample_state",
            secret_key=FYERS_SECRET_KEY,
            grant_type="authorization_code"
        )

        appSession.set_token(auth_code)
        response = appSession.generate_token()

        if "access_token" in response:
            access_token = response["access_token"]
            token_storage["access_token"] = access_token

            # Initialize Fyers model for API calls
            fyers = fyersModel.FyersModel(
                token=access_token,
                is_async=False,
                client_id=FYERS_CLIENT_ID,
                log_path=""
            )

            return {
                "status": "success",
                "message": "Access token generated successfully",
                "access_token": access_token,
                "token_type": "Bearer"
            }
        else:
            raise HTTPException(
                status_code=400,
                detail=f"Token generation failed: {response}"
            )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Token generation failed: {str(e)}")

@app.get("/profile")
async def get_profile():
    """
    Get user profile information using access token
    """
    try:
        access_token = token_storage.get("access_token")

        if not access_token:
            raise HTTPException(
                status_code=401,
                detail="No access token available. Please generate token first"
            )

        # Initialize Fyers model
        fyers = fyersModel.FyersModel(
            token=access_token,
            is_async=False,
            client_id=FYERS_CLIENT_ID,
            log_path=""
        )

        # Get profile data
        profile_data = fyers.get_profile()

        return {
            "status": "success",
            "profile": profile_data
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Profile fetch failed: {str(e)}")

@app.get("/market-status")
async def get_market_status():
    """
    Get current market status
    """
    try:
        access_token = token_storage.get("access_token")

        if not access_token:
            raise HTTPException(
                status_code=401,
                detail="No access token available. Please generate token first"
            )

        fyers = fyersModel.FyersModel(
            token=access_token,
            is_async=False,
            client_id=FYERS_CLIENT_ID,
            log_path=""
        )

        market_data = fyers.market_status()

        return {
            "status": "success",
            "market_status": market_data
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Market status fetch failed: {str(e)}")

@app.get("/logout")
async def logout():
    """
    Clear stored tokens (logout)
    """
    token_storage.clear()
    return {"status": "success", "message": "Tokens cleared successfully"}

if __name__ == "__main__":
    uvicorn.run(
        "main:app",  # Replace with your filename if different
        host="0.0.0.0",
        port=8000,
        reload=True
    )