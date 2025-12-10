import requests
from typing import Optional, Dict, List, Any

class ProwlarrClient:
    def __init__(self, base_url: str, api_key: str, timeout: int = 30):
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({"X-Api-Key": self.api_key, "Content-Type": "application/json"})
        self.timeout = timeout

    def _url(self, path: str) -> str:
        return f"{self.base_url}/api/v1/{path.lstrip('/')}"

    def get_indexers(self) -> List[Dict[str, Any]]:
        r = self.session.get(self._url('indexer'))
        r.raise_for_status()
        data = r.json()
        # sometimes the API returns a wrapper object
        if isinstance(data, dict):
            for key in ('records', 'items', 'results'):
                if key in data and isinstance(data[key], list):
                    return data[key]
            # if dict but looks like an indexer list, try to return list of values
            if isinstance(data.get('result'), list):
                return data.get('result')
            if isinstance(data.get('items'), list):
                return data.get('items')
            # maybe data is a single indexer
            return [data]
        if isinstance(data, list):
            # return only dict entries (sometimes APIs include strings)
            return [d for d in data if isinstance(d, dict)]
        return data

    def get_indexer_statuses(self) -> List[Dict[str, Any]]:
        r = self.session.get(self._url('indexerstatus'))
        r.raise_for_status()
        data = r.json()
        if isinstance(data, list):
            return [d for d in data if isinstance(d, dict)]
        return data

    def get_indexer(self, idx_id: int) -> Dict[str, Any]:
        r = self.session.get(self._url(f'indexer/{idx_id}'))
        r.raise_for_status()
        return r.json()

    def test_indexer(self, indexer_obj: Dict[str, Any]) -> Dict[str, Any]:
        r = self.session.post(self._url('indexer/test'), json=indexer_obj, timeout=self.timeout)
        # Server returns JSON describing result in many cases; include body for debugging on errors
        try:
            r.raise_for_status()
        except Exception as e:
            # include response text up to safe size for debugging
            text = None
            try:
                text = r.text
            except Exception:
                text = '<unavailable>'
            raise RuntimeError(f"Test indexer failed: HTTP {r.status_code}: {text}") from e
        # If JSON, return dict/list; otherwise return raw text for diagnostics
        try:
            return r.json()
        except Exception:
            return { 'response_text': r.text }

    def update_indexer(self, idx_id: int, indexer_obj: Dict[str, Any]) -> Dict[str, Any]:
        r = self.session.put(self._url(f'indexer/{idx_id}'), json=indexer_obj, timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    def get_tags(self) -> List[Dict[str, Any]]:
        r = self.session.get(self._url('tag'))
        r.raise_for_status()
        return r.json()

    def create_tag(self, label: str) -> Dict[str, Any]:
        r = self.session.post(self._url('tag'), json={'label': label})
        r.raise_for_status()
        return r.json()

    def find_or_create_tag(self, label: str) -> Dict[str, Any]:
        tags = self.get_tags()
        for t in tags:
            if t.get('label') == label:
                return t
        return self.create_tag(label)
