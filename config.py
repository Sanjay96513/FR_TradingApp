from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Application settings are managed by this class.
    It reads environment variables and provides them to the application.
    """
    FYERS_CLIENT_ID: str = "XCXXXXXxxM-100"
    FYERS_SECRET_KEY: str = "MH*****TJ5"
    FYERS_REDIRECT_URI: str = "http://localhost:8000/callback"
    FYERS_STATE: str = "sample_state"

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")


settings = Settings()