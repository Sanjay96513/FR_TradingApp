from fastapi import FastAPI, HTTPException, Query, Depends
from typing import Annotated
import uvicorn

from services import FyersService, TokenStore
from config import settings

app = FastAPI(
    title="Fyers API Integration",
    version="2.0.0",
    description="A refactored, service-oriented FastAPI application for Fyers API.",
)

# Dependency Injection Setup
token_store = TokenStore()

def get_fyers_service() -> FyersService:
    """Dependency provider for the FyersService."""
    return FyersService(token_store=token_store)

FyersServiceDep = Annotated[FyersService, Depends(get_fyers_service)]

@app.get("/")
async def root():
    """Root endpoint to check service status."""
    return {"message": "Fyers API Integration Service", "status": "running"}

@app.get("/login")
async def login_fyers(fyers_service: FyersServiceDep):
    """
    Initiates the Fyers login flow by providing an authentication URL.
    """
    try:
        auth_url = fyers_service.generate_auth_code_url()
        return {
            "status": "success",
            "message": "Please visit the URL to authenticate",
            "auth_url": auth_url,
            "next_step": f"After authentication, you will be redirected to the callback URL: {settings.FYERS_REDIRECT_URI}",
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Login initiation failed: {e}")

@app.get("/callback")
async def auth_callback(
    fyers_service: FyersServiceDep,
    code: str = Query(..., description="Authorization code from Fyers"),
    state: str = Query(..., description="State parameter for CSRF protection"),
):
    """
    Callback endpoint for Fyers OAuth redirect. This should match your registered redirect_uri.
    """
    if state != settings.FYERS_STATE:
        raise HTTPException(status_code=400, detail="Invalid state parameter. Possible CSRF attack.")

    fyers_service.token_store.set_auth_code(code)
    return {
        "status": "success",
        "message": "Authorization code received successfully. You can now generate an access token.",
        "auth_code": code,
        "next_step": "Call /generate-token to get the access token.",
    }

@app.get("/generate-token")
async def generate_token(fyers_service: FyersServiceDep):
    """
    Generates an access token using the stored authorization code.
    """
    auth_code = fyers_service.token_store.get_auth_code()
    if not auth_code:
        raise HTTPException(
            status_code=400,
            detail="No authorization code found. Please complete the login flow via /login first.",
        )
    try:
        access_token = fyers_service.generate_access_token(auth_code)
        return {
            "status": "success",
            "message": "Access token generated successfully.",
            "access_token": access_token,
            "token_type": "Bearer",
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Token generation failed: {e}")

@app.get("/profile")
async def get_profile(fyers_service: FyersServiceDep):
    """
    Fetches the user's profile information from Fyers.
    """
    try:
        profile_data = fyers_service.get_profile()
        return {"status": "success", "profile": profile_data}
    except PermissionError as e:
        raise HTTPException(status_code=401, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Profile fetch failed: {e}")

@app.get("/market-status")
async def get_market_status(fyers_service: FyersServiceDep):
    """
    Fetches the current market status from Fyers.
    """
    try:
        market_data = fyers_service.get_market_status()
        return {"status": "success", "market_status": market_data}
    except PermissionError as e:
        raise HTTPException(status_code=401, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Market status fetch failed: {e}")

@app.get("/logout")
async def logout(fyers_service: FyersServiceDep):
    """
    Clears the stored authentication and access tokens.
    """
    fyers_service.token_store.clear()
    return {"status": "success", "message": "Tokens cleared successfully (logged out)."}

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
    )