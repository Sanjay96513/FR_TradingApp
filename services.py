from fyers_apiv3 import fyersModel
from config import settings

class TokenStore:
    """A simple in-memory store for the authorization and access tokens."""
    def __init__(self):
        self._auth_code: str | None = None
        self._access_token: str | None = None

    def set_auth_code(self, code: str):
        self._auth_code = code

    def get_auth_code(self) -> str | None:
        return self._auth_code

    def set_access_token(self, token: str):
        self._access_token = token

    def get_access_token(self) -> str | None:
        return self._access_token

    def clear(self):
        self._auth_code = None
        self._access_token = None

class FyersService:
    """
    Service layer to encapsulate all business logic for interacting with the Fyers API.
    """
    def __init__(self, token_store: TokenStore):
        self.token_store = token_store
        self.session_model = fyersModel.SessionModel(
            client_id=settings.FYERS_CLIENT_ID,
            secret_key=settings.FYERS_SECRET_KEY,
            redirect_uri=settings.FYERS_REDIRECT_URI,
            response_type="code",
            grant_type="authorization_code",
            state=settings.FYERS_STATE
        )

    def generate_auth_code_url(self) -> str:
        """Generates the authentication URL for the user to log in."""
        return self.session_model.generate_authcode()

    def generate_access_token(self, auth_code: str) -> str:
        """Generates an access token from an authorization code."""
        self.session_model.set_token(auth_code)
        response = self.session_model.generate_token()
        if "access_token" not in response:
            raise ValueError(f"Token generation failed: {response}")
        access_token = response["access_token"]
        self.token_store.set_access_token(access_token)
        return access_token

    def _get_fyers_model(self) -> fyersModel.FyersModel:
        """Initializes and returns a FyersModel instance for API calls."""
        access_token = self.token_store.get_access_token()
        if not access_token:
            raise PermissionError("Access token not available. Please authenticate.")

        return fyersModel.FyersModel(
            token=access_token,
            is_async=False,
            client_id=settings.FYERS_CLIENT_ID,
            log_path=""
        )

    def get_profile(self) -> dict:
        """Fetches the user profile from Fyers."""
        fyers = self._get_fyers_model()
        return fyers.get_profile()

    def get_market_status(self) -> dict:
        """Fetches the market status from Fyers."""
        fyers = self._get_fyers_model()
        return fyers.market_status()