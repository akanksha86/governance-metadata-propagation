import contextvars
from typing import Optional
import google.oauth2.credentials

_oauth_token = contextvars.ContextVar("oauth_token", default=None)

def set_oauth_token(token: Optional[str]):
    """Sets the OAuth token for the current context."""
    _oauth_token.set(token)

def get_oauth_token() -> Optional[str]:
    """Gets the OAuth token from the current context."""
    return _oauth_token.get()

def get_credentials(quota_project_id: str) -> Optional[google.oauth2.credentials.Credentials]:
    """
    Returns Google Credentials object created from the stored OAuth token.
    IMPORTANT: quota_project_id is required for user credentials to work with certain BigQuery APIs.
    """
    token = get_oauth_token()
    if token:
        return google.oauth2.credentials.Credentials(
            token,
            quota_project_id=quota_project_id
        )
    return None
