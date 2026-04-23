"""Web entrypoint for the internal review UI."""

from src.web.app import create_app, serve

__all__ = ["create_app", "serve"]
