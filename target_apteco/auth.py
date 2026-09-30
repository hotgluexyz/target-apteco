"""Apteco Orbit API authentication."""

from __future__ import annotations

import time
from typing import Any, Dict, Optional
from urllib.parse import urljoin

import requests


class AptecoAuthenticator:
    """Authenticate against OrbitAPI SimpleLogin and cache a Bearer token."""

    def __init__(self, config: Dict[str, Any]) -> None:
        self._config = config
        self._access_token: Optional[str] = None
        self._session_id: Optional[str] = None
        self._expires_at: float = 0

    @property
    def orbit_api_url(self) -> str:
        base = self._config.get("base_url", "https://hotglue.ca-1.apteco.cloud").rstrip("/")
        return self._config.get("orbit_api_url") or f"{base}/OrbitAPI"

    @property
    def data_view_name(self) -> str:
        return self._config.get("data_view_name", "DB01")

    def is_token_valid(self) -> bool:
        return bool(self._access_token) and time.time() < (self._expires_at - 30)

    def update_access_token(self) -> None:
        username = self._config.get("username")
        password = self._config.get("password")
        if not username or not password:
            raise ValueError("username and password are required")

        url = urljoin(self.orbit_api_url.rstrip("/") + "/", f"{self.data_view_name}/Sessions/SimpleLogin")
        response = requests.post(
            url,
            data={
                "UserLogin": username,
                "Password": password,
                "ClientType": "Orbit",
            },
            headers={"Accept": "application/json"},
            timeout=60,
        )
        response.raise_for_status()
        payload = response.json()
        self._access_token = payload["accessToken"]
        self._session_id = payload.get("sessionId")
        # Orbit tokens are short-lived (~5 minutes); refresh proactively.
        self._expires_at = time.time() + 240

    @property
    def auth_headers(self) -> Dict[str, str]:
        if not self.is_token_valid():
            self.update_access_token()
        return {
            "Authorization": f"Bearer {self._access_token}",
            "Accept": "application/json",
        }
