import contextvars

_oauth_token = contextvars.ContextVar("oauth_token", default=None)

def set_oauth_token(token: str):
    _oauth_token.set(token)

def get_oauth_token() -> str:
    return _oauth_token.get()
