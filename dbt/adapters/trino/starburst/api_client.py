import time

import requests
from dbt.adapters.events.logging import AdapterLogger

from dbt.adapters.trino.__version__ import version

logger = AdapterLogger("Trino")

STARBURST_API_TIMEOUT = 30
TOKEN_EXPIRY_BUFFER_SECONDS = 60
# Default column descriptions the batch endpoint accepts per request.
DEFAULT_MAX_COLUMN_DESCRIPTIONS_PER_REQUEST = 100
CLIENT_INFO_HEADER = "X-Client-Info"


def _extract_results(data) -> list:
    """Extract the results list from a Starburst API response.

    Responses are either a bare list or a dict with a "result" key.
    """
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return data.get("result", [])
    return []


def _find_id(items: list, target_name: str, name_key: str, id_key: str) -> str | None:
    """Find an ID by name in a list of dicts, trying primary then fallback field names."""
    for item in items:
        if not isinstance(item, dict):
            continue
        name = item.get(name_key) or item.get("name")
        item_id = item.get(id_key) or item.get("id")
        if name == target_name and item_id:
            return str(item_id)
    return None


def _chunked(items: list, size: int):
    """Yield successive chunks of at most ``size`` items."""
    for start in range(0, len(items), size):
        yield items[start : start + size]


class StarburstDiscoveryClient:
    def __init__(
        self,
        starburst_url: str,
        client_id: str,
        secret_key: str,
        max_column_batch_size: int = DEFAULT_MAX_COLUMN_DESCRIPTIONS_PER_REQUEST,
    ):
        base = starburst_url.rstrip("/")
        self.base_url = f"{base}/public/api/v1"
        self._token_url = f"{base}/oauth/v2/token"
        self._client_id = client_id
        self._secret_key = secret_key
        self._max_column_batch_size = max_column_batch_size
        self._catalog_ids: dict[str, str | None] = {}
        self._access_token: str | None = None
        self._token_expires_at: float = 0
        self._session = requests.Session()
        self._session.headers.update({CLIENT_INFO_HEADER: f"dbt-trino/{version}"})

    def _ensure_token(self) -> bool:
        if self._access_token and time.time() < self._token_expires_at:
            return True
        try:
            response = self._session.post(
                self._token_url,
                auth=(self._client_id, self._secret_key),
                data={"grant_type": "client_credentials"},
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=STARBURST_API_TIMEOUT,
            )
            response.raise_for_status()
            token_data = response.json()
            self._access_token = token_data["access_token"]
            expires_in = token_data.get("expires_in", 600)
            self._token_expires_at = time.time() + expires_in - TOKEN_EXPIRY_BUFFER_SECONDS
            self._session.headers.update({"Authorization": f"Bearer {self._access_token}"})
            return True
        except requests.RequestException as e:
            logger.warning(f"Failed to obtain Starburst OAuth2 token: {e}")
            return False

    def _api_request(self, method: str, path: str, body: dict | None = None) -> list:
        """Make an authenticated API request and return the extracted results.

        Raises requests.RequestException on failure.
        """
        self._ensure_token()
        url = f"{self.base_url}{path}"
        kwargs: dict[str, int | dict] = {"timeout": STARBURST_API_TIMEOUT}
        if body is not None:
            kwargs["json"] = body
        response = getattr(self._session, method)(url, **kwargs)
        response.raise_for_status()
        logger.debug(f"Starburst {method.upper()} {path} status: {response.status_code}")
        # Write endpoints (e.g. the batch column update) return 204 No Content
        # with an empty body; there is nothing to parse in that case.
        if response.status_code == 204 or not response.content:
            return []
        data = response.json()
        logger.debug(f"Starburst {method.upper()} {path} response: {data}")
        return _extract_results(data)

    def _api_get(self, path: str) -> list:
        """GET a Starburst API endpoint and return the results list."""
        return self._api_request("get", path)

    def _api_patch(self, path: str, body: dict) -> list:
        """PATCH a Starburst API endpoint and return the extracted results."""
        return self._api_request("patch", path, body)

    # -- Catalog ID resolution --

    def _get_catalog_id(self, catalog_name: str) -> str | None:
        if catalog_name not in self._catalog_ids:
            self._catalog_ids[catalog_name] = self._resolve_catalog_id(catalog_name)
        return self._catalog_ids[catalog_name]

    def _resolve_catalog_id(self, catalog_name: str) -> str | None:
        try:
            results = self._api_get("/catalog")
        except requests.RequestException as e:
            logger.warning(f"Failed to resolve Starburst catalog ID: {e}")
            return None
        cat_id = _find_id(results, catalog_name, "catalogName", "catalogId")
        if cat_id is None:
            logger.warning(
                f"Starburst catalog '{catalog_name}' not found. "
                "Starburst description sync will be skipped."
            )
        return cat_id

    # -- Public methods --

    def update_table_description(
        self, catalog_name: str, schema_name: str, table_name: str, description: str
    ) -> list | None:
        catalog_id = self._get_catalog_id(catalog_name)
        if catalog_id is None:
            return None
        path = f"/catalog/{catalog_id}/schema/{schema_name}/table/{table_name}"
        return self._api_patch(path, {"description": description})

    def update_column_description(
        self,
        catalog_name: str,
        schema_name: str,
        table_name: str,
        column_name: str,
        description: str,
    ) -> list | None:
        catalog_id = self._get_catalog_id(catalog_name)
        if catalog_id is None:
            return None
        path = (
            f"/catalog/{catalog_id}"
            f"/schema/{schema_name}/table/{table_name}/column/{column_name}"
        )
        return self._api_patch(path, {"description": description})

    def update_column_descriptions(
        self,
        catalog_name: str,
        schema_name: str,
        table_name: str,
        descriptions: dict,
    ) -> None:
        """Update descriptions for multiple columns via the batch endpoint.

        Sends the descriptions in chunks of at most ``max_column_batch_size``
        columns to the collection path.
        """
        if not descriptions:
            return
        catalog_id = self._get_catalog_id(catalog_name)
        if catalog_id is None:
            return
        path = f"/catalog/{catalog_id}/schema/{schema_name}/table/{table_name}/column"
        for chunk in _chunked(list(descriptions.items()), self._max_column_batch_size):
            self._api_patch(path, {"descriptions": dict(chunk)})
