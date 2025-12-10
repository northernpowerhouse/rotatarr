#!/usr/bin/env python3
import os
import time
import copy
import logging
import json
from typing import List, Dict, Any, Optional

from prowlarr_client import ProwlarrClient
# NOTE: Environment variables come from docker-compose.yml only (no .env file)

PROWLARR_URL = os.environ.get('PROWLARR_URL')
PROWLARR_API_KEY = os.environ.get('PROWLARR_API_KEY')
CHECK_INTERVAL_MIN = int(os.environ.get('CHECK_INTERVAL_MIN', '30'))
TEST_RETRIES = int(os.environ.get('TEST_RETRIES', '2'))
TEST_RETRY_DELAY_SEC = int(os.environ.get('TEST_RETRY_DELAY_SEC', '2'))
TAG_TO_TRY = os.environ.get('TAG_TO_TRY', '')
TAG_FORCE = os.environ.get('TAG_FORCE', 'false').lower() in ('1', 'true', 'yes')
DRY_RUN = os.environ.get('DRY_RUN', 'true').lower() in ('1', 'true', 'yes')
APPLY_TAG_SAVE_BEFORE_TEST = os.environ.get('APPLY_TAG_SAVE_BEFORE_TEST', 'false').lower() in ('1','true','yes')
FORCE_TEST_INDEXERS = os.environ.get('FORCE_TEST_INDEXERS', '')
LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')
ONE_SHOT = os.environ.get('ONE_SHOT', 'false').lower() in ('1', 'true', 'yes')
INDEXER_MAX_ATTEMPTS = int(os.environ.get('INDEXER_MAX_ATTEMPTS', '3'))
INDEXER_COOLDOWN_MIN = int(os.environ.get('INDEXER_COOLDOWN_MIN', '60'))
INDEXER_STATE_FILE = os.environ.get('INDEXER_STATE_FILE', '/app/data/indexer_state.json')
TEST_AS_UI = os.environ.get('TEST_AS_UI', 'false').lower() in ('1','true','yes')
INSPECT_INDEXERS = os.environ.get('INSPECT_INDEXERS', 'false').lower() in ('1','true','yes')
DUMP_INDEXERS = os.environ.get('DUMP_INDEXERS', '')

logging.basicConfig(level=LOG_LEVEL, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger('rotatarr')

def make_client() -> Optional[ProwlarrClient]:
    url = os.environ.get('PROWLARR_URL')
    api_key = os.environ.get('PROWLARR_API_KEY')
    if not url or not api_key:
        logger.error('PROWLARR_URL and PROWLARR_API_KEY must be set to use the API. Some functions will still work locally.')
        return None
    return ProwlarrClient(url, api_key)


def _load_indexer_state() -> Dict[str, Any]:
    state = {}
    try:
        if os.path.exists(INDEXER_STATE_FILE):
            with open(INDEXER_STATE_FILE, 'r') as fh:
                state = json.load(fh) or {}
    except Exception as e:
        logger.debug(f"Unable to load indexer state from {INDEXER_STATE_FILE}: {e}")
    return state


def _save_indexer_state(state: Dict[str, Any]) -> None:
    try:
        os.makedirs(os.path.dirname(INDEXER_STATE_FILE), exist_ok=True)
        with open(INDEXER_STATE_FILE, 'w') as fh:
            json.dump(state, fh)
    except Exception as e:
        logger.warning(f"Failed to save indexer state to {INDEXER_STATE_FILE}: {e}")


def is_indexer_error(indexer: Dict[str, Any]) -> bool:
    # heuristics: look for common error fields
    # Older API / variations might be a string state
    state_field = indexer.get('status') or indexer.get('state') or indexer.get('Status')
    if isinstance(state_field, str) and state_field.lower() == 'error':
        return True
    # Schema based: 'status' is a dict (IndexerStatusResource)
    if isinstance(state_field, dict):
        most_recent = state_field.get('mostRecentFailure') or state_field.get('initialFailure')
        if most_recent:
            return True
    # Some objs include a configContract string rather than dict; skip
    config_contract = indexer.get('configContract') if isinstance(indexer, dict) else None
    if isinstance(config_contract, dict) and config_contract.get('name') == 'error':
        return True
    # 'error' message
    # provider message (ProviderMessage) may be in 'message' property and include a type
    if isinstance(indexer, dict) and (indexer.get('error') or indexer.get('errors') or indexer.get('LastException')):
        return True
    # Another heuristic: last update error message
    if isinstance(indexer, dict) and indexer.get('lastError'):
        return True
    # Heuristic: indexer has no definition - Prowlarr will say it "has no definition and will not work"
    # Common definition fields: 'definition', 'definitionId', 'definitionUid', 'implementation'
    def_fields = ('definition', 'definitionId', 'definitionUid', 'implementation')
    missing_def_count = sum(1 for k in def_fields if not indexer.get(k))
    # if all definition-like fields are missing or empty, flag it as an error
    if missing_def_count == len(def_fields):
        return True
    # Additionally inspect fields array for 'definitionFile' or similar settings that are empty
    fields = indexer.get('fields')
    if isinstance(fields, list):
        for f in fields:
            if isinstance(f, dict) and f.get('name') and f.get('name').lower() == 'definitionfile':
                val = f.get('value')
                if not val and val != 0:
                    return True
    # check provider message type 'error'
    message = indexer.get('message') if isinstance(indexer, dict) else None
    if isinstance(message, dict) and message.get('type') == 'error':
        return True
    return False


def get_alternate_base_urls(indexer: Dict[str, Any]) -> List[str]:
    candidates = []
    # Common Prowlarr indexer fields
    # Try Settings.BaseUrl or Config.Settings.BaseUrl
    settings = indexer.get('settings') or indexer.get('Settings') or indexer.get('Settings', {})
    if isinstance(settings, dict):
        baseurl = settings.get('baseUrl') or settings.get('BaseUrl') or settings.get('BaseUrl')
        if baseurl:
            candidates.append(baseurl)
    # Try 'indexerUrls', 'legacyUrls', 'urls', 'alternateUrls'
    urls = indexer.get('indexerUrls') or indexer.get('indexerurls') or indexer.get('urls') or indexer.get('Urls') or indexer.get('alternateUrls') or indexer.get('AlternateUrls') or indexer.get('legacyUrls') or indexer.get('legacyurls')
    if isinstance(urls, list):
        candidates.extend([u for u in urls if isinstance(u, str) and u])
    # Some definitions store config in 'config' or 'settings
    config = indexer.get('config') or indexer.get('Config') or indexer.get('configContract') or {}
    # If there are config fields with 'baseUrl', add them
    if isinstance(config, dict):
        for v in config.values():
            if isinstance(v, str) and v.startswith('http'):
                candidates.append(v)
    # Fields entries can include URLs
    fields = indexer.get('fields')
    if isinstance(fields, list):
        for f in fields:
            if isinstance(f, dict):
                v = f.get('value') or f.get('Value')
                if isinstance(v, str) and v.startswith('http'):
                    candidates.append(v)
    # Deduplicate and return
    out = []
    for c in candidates:
        if c not in out:
            out.append(c)
    return out


def build_ui_test_payload(indexer: Dict[str, Any]) -> Dict[str, Any]:
    """Return a minimal payload resembling what the UI would send when testing.
    This payload includes only the most common fields Prowlarr cares about, reducing
    noise and making it more likely to follow the UI path that may use proxies or
    other server-configured helpers.
    """
    out = {}
    # Basic identity fields
    for k in ('id', 'name', 'implementation', 'definitionId', 'definitionUid'):
        if k in indexer:
            out[k] = indexer[k]
    # include settings (particularly baseUrl fields)
    if 'settings' in indexer and isinstance(indexer['settings'], dict):
        out['settings'] = indexer['settings']
    # include configured fields (field/value pairs)
    if 'fields' in indexer and isinstance(indexer['fields'], list):
        out['fields'] = indexer['fields']
    # include URL lists
    if 'indexerUrls' in indexer and isinstance(indexer['indexerUrls'], list):
        out['indexerUrls'] = indexer['indexerUrls']
    if 'legacyUrls' in indexer and isinstance(indexer['legacyUrls'], list):
        out['legacyUrls'] = indexer['legacyUrls']
    return out


def set_base_url(indexer: Dict[str, Any], url: str) -> None:
    # attempt common fields to set
    if 'settings' in indexer and isinstance(indexer['settings'], dict):
        if 'baseUrl' in indexer['settings'] or 'BaseUrl' in indexer['settings']:
            if 'baseUrl' in indexer['settings']:
                indexer['settings']['baseUrl'] = url
            if 'BaseUrl' in indexer['settings']:
                indexer['settings']['BaseUrl'] = url
            return
        else:
            indexer['settings']['baseUrl'] = url
            return
    if 'config' in indexer and isinstance(indexer['config'], dict):
        # Objective: create or replace any base url key that looks like base url
        for k in ['baseUrl', 'BaseUrl', 'url', 'Url']:
            if k in indexer['config']:
                indexer['config'][k] = url
                return
        # fallback
        indexer['config']['baseUrl'] = url
        return
    # last fallback: set top-level 'url' or 'BaseUrl'
    if 'BaseUrl' in indexer:
        indexer['BaseUrl'] = url
        return
    indexer['BaseUrl'] = url

    # Replace in indexerUrls and legacyUrls arrays
    if 'indexerUrls' in indexer and isinstance(indexer['indexerUrls'], list):
        indexer['indexerUrls'][0:1] = [url]
        return
    if 'legacyUrls' in indexer and isinstance(indexer['legacyUrls'], list):
        indexer['legacyUrls'][0:1] = [url]
        return
    # Also attempt to replace any URL-like field in fields
    if 'fields' in indexer and isinstance(indexer['fields'], list):
        for f in indexer['fields']:
            if isinstance(f, dict) and isinstance(f.get('value'), str) and f['value'].startswith('http'):
                f['value'] = url
                return


def add_tag_to_indexer(indexer: Dict[str, Any], tag: Any) -> None:
    """Add a tag (either id or tag object) to an indexer object payload.
    Ensures both 'tagIds' (list of ints) and 'tags' (list of objects) are set to maximize Prowlarr compatibility.
    """
    tag_id = None
    tag_label = None
    if isinstance(tag, dict):
        tag_id = tag.get('id')
        tag_label = tag.get('label') or tag.get('name')
    else:
        tag_id = tag
    # Add numeric tag id to tagIds
    if tag_id is not None:
        try:
            tag_id_int = int(tag_id)
        except Exception:
            tag_id_int = None
    else:
        tag_id_int = None
    if tag_id_int is not None:
        if 'tagIds' in indexer and isinstance(indexer['tagIds'], list):
            if tag_id_int not in indexer['tagIds']:
                indexer['tagIds'].append(tag_id_int)
        else:
            indexer['tagIds'] = [tag_id_int]
    # Also add an object to 'tags' list for UI / object-based expectations
    if tag_label and tag_id_int is not None:
        tag_obj = {'id': tag_id_int, 'label': tag_label}
        if 'tags' in indexer and isinstance(indexer['tags'], list):
            if not any(isinstance(t, dict) and t.get('id') == tag_id_int for t in indexer['tags']):
                indexer['tags'].append(tag_obj)
        else:
            indexer['tags'] = [tag_obj]


def run_once(client: Optional[ProwlarrClient] = None):
    if client is None:
        client = make_client()
    if client is None:
        logger.error('No Prowlarr client configured; run in DRY_RUN or set PROWLARR_URL/PROWLARR_API_KEY')
        return {'fixed': [], 'skipped': [], 'failed': []}
    try:
        indexers = client.get_indexers()
        statuses = client.get_indexer_statuses()
        status_map = {s.get('indexerId'): s for s in statuses if isinstance(s, dict)}
    except Exception as e:
        logger.exception(f'Failed to retrieve indexers list from Prowlarr: {e}')
        return {'fixed': [], 'skipped': [], 'failed': []}
    logger.info(f'Found {len(indexers)} indexers')
    # Optional diagnostic: print per-indexer key sets and frequency summary
    if INSPECT_INDEXERS:
        key_counts = {}
        indexer_keys = {}
        for idx in indexers:
            idx_id = idx.get('id') or idx.get('Id') or '<no-id>'
            name = idx.get('name') or idx.get('Name') or '<no-name>'
            keys = [k for k, v in idx.items() if v is not None]
            indexer_keys[str(idx_id)] = {'name': name, 'keys': keys}
            for k in keys:
                key_counts[k] = key_counts.get(k, 0) + 1
        logger.info('Indexer key frequency summary:')
        for k, cnt in sorted(key_counts.items(), key=lambda x: (-x[1], x[0])):
            logger.info(f"  {k}: {cnt}/{len(indexers)}")
        logger.info('Indexers and their keys (showing only keys with lower frequency):')
        for idx_id, info in indexer_keys.items():
            unique_keys = [k for k in info['keys'] if key_counts.get(k, 0) < len(indexers)]
            logger.info(f"  {info['name']} ({idx_id}): {unique_keys}")
        # Show values for common definition fields to identify indexers that have 'no definition'
        def_fields = ('definition', 'definitionId', 'definitionUid', 'definitionName', 'implementation', 'implementationName')
        logger.info('Indexers missing definition-like values:')
        for k in def_fields:
            missing = []
            for idx in indexers:
                val = idx.get(k)
                # treat falsy or empty strings/lists/dicts as missing
                if not val and val != 0:
                    missing.append(idx.get('name') or idx.get('Name') or idx.get('id') or '<no-name>')
            if missing:
                logger.info(f"  {k}: {len(missing)} indexers: {', '.join(missing)}")
        # Exit early for inspection
        # Optionally dump full JSON for specific indexers if requested
        if DUMP_INDEXERS:
            dump_names = [n.strip().lower() for n in DUMP_INDEXERS.split(',') if n.strip()]
            for idx in indexers:
                idx_name = (idx.get('name') or idx.get('Name') or str(idx.get('id'))).lower()
                if idx_name in dump_names or str(idx.get('id')) in dump_names:
                    try:
                        logger.info(f"Dumping indexer {idx.get('name')} ({idx.get('id')}): {json.dumps(idx, indent=2)[:10000]}")
                    except Exception:
                        logger.info(f"Dumping indexer {idx.get('name')} ({idx.get('id')}) (undumpable due to size or encoding)")
        # Dump status objects for these names too to see if Prowlarr marked them as lacking definitions
        try:
            statuses = client.get_indexer_statuses()
            status_map = {s.get('indexerId'): s for s in statuses if isinstance(s, dict)}
            if DUMP_INDEXERS:
                for idx in indexers:
                    idx_id = idx.get('id')
                    idx_name = (idx.get('name') or idx.get('Name') or str(idx_id)).lower()
                    if idx_name in dump_names or str(idx_id) in dump_names:
                        st = status_map.get(idx_id)
                        if st:
                            try:
                                logger.info(f"Dumping indexer status for {idx.get('name')} ({idx_id}): {json.dumps(st, indent=2)}")
                            except Exception:
                                logger.info(f"Dumping indexer status for {idx.get('name')} ({idx_id}) (undumpable)")
        except Exception:
            pass
        return {'fixed': [], 'skipped': [], 'failed': []}
    results = {'fixed': [], 'skipped': [], 'failed': []}
    # Load per-indexer state (cooldown/failure counters)
    indexer_state = _load_indexer_state()
    for idx in indexers:
        idx_id = idx.get('id') or idx.get('Id')
        if not idx_id:
            logger.debug('Indexer missing id; skipping')
            results['skipped'].append(idx)
            continue
        # Prefer indexer status endpoint for error detection
        s = status_map.get(idx_id)
        if s and (s.get('mostRecentFailure') or s.get('disabledTill')):
            logger.info(f"Indexer {idx.get('name', idx_id)} ({idx_id}) has failure status; attempting to recover")
        else:
            # fallback to existing heuristics
            # Only skip if we don't have a status showing failures AND heuristics don't detect an error
            if not (s and (s.get('mostRecentFailure') or s.get('disabledTill'))) and not is_indexer_error(idx):
                logger.debug(f"Indexer {idx.get('name', idx_id)} ({idx_id}) not marked as error; skipping")
                continue
        if not isinstance(idx, dict):
            logger.warning(f"Index entry is not a dict; skipping: {idx}")
            results['skipped'].append(idx)
            continue
        try:
            idx_id = idx.get('id') or idx.get('Id')
            if not idx_id:
                logger.debug('Indexer missing id; skipping')
                results['skipped'].append(idx)
                continue
            # At this point we know indexer either has a status-based failure or heuristics indicate an error
            logger.info(f"Indexer {idx.get('name', idx_id)} ({idx_id}) appears to be in error; attempting to recover")
            original = copy.deepcopy(idx)
            base_urls = get_alternate_base_urls(idx)
            logger.debug(f"Candidate base URLs for {idx.get('name', idx_id)}: {base_urls}")
            if not base_urls:
                logger.info(f"No base URL candidates for indexer {idx.get('name', idx_id)}; skipping")
                results['skipped'].append(idx)
                continue
            updated = False
            tag_obj = None
            if TAG_TO_TRY and TAG_FORCE:
                # find or create tag
                if not DRY_RUN:
                    tag_obj = client.find_or_create_tag(TAG_TO_TRY)
                else:
                    # pretend id is a placeholder
                    tag_obj = {'id': -1, 'label': TAG_TO_TRY}
                logger.info(f"Using tag {tag_obj}")

            # check if indexer is in cooldown
            state_key = str(idx_id)
            now_ts = int(time.time())
            idx_state = indexer_state.get(state_key, {})
            next_allowed_at = idx_state.get('next_allowed_at')
            # check forced tests override
            forced_names = [n.strip().lower() for n in FORCE_TEST_INDEXERS.split(',') if n.strip()]
            forced_by_name = idx.get('name') and idx.get('name').lower() in forced_names
            forced_by_id = str(idx_id) in forced_names
            if next_allowed_at and now_ts < next_allowed_at and not (forced_by_name or forced_by_id):
                logger.info(f"Indexer {idx.get('name', idx_id)} ({idx_id}) is in cooldown until {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(next_allowed_at))}; skipping")
                results['skipped'].append(idx)
                continue

            # helper to perform tests with retries and determine ok
            def _perform_test_with_retries(client, test_obj, ref_id, label) -> bool:
                """Return True if test succeeds, False otherwise; logs and raises on hard failures."""
                test_res = None
                for attempt in range(0, TEST_RETRIES + 1):
                    try:
                        test_res = client.test_indexer(test_obj)
                        # Determine success now
                        ok_local = False
                        if isinstance(test_res, dict):
                            ok_local = test_res.get('success') is True or test_res.get('isSuccess') is True
                            if not ok_local and any(v is True for v in test_res.values() if isinstance(v, bool)):
                                ok_local = True
                        elif isinstance(test_res, list) and len(test_res) > 0:
                            for r in test_res:
                                if isinstance(r, dict) and (r.get('success') is True or r.get('status') == 'Success'):
                                    ok_local = True
                                    break
                        if ok_local:
                            return True
                        # if not ok and not transient, no need to retry
                        logger.debug(f"Test attempt returned not-ok for {ref_id} ({label}): {test_res}")
                        if TEST_AS_UI:
                            try:
                                ui_payload = build_ui_test_payload(test_obj if isinstance(test_obj, dict) else {})
                                if ui_payload and ui_payload != test_obj:
                                    logger.info(f"Attempting UI-like minimal payload test for {ref_id} ({label})")
                                    # Try UI-like payload without additional retries here; let the caller's loop handle retries
                                    ui_res = client.test_indexer(ui_payload)
                                    ui_ok = False
                                    if isinstance(ui_res, dict):
                                        ui_ok = ui_res.get('success') is True or ui_res.get('isSuccess') is True
                                    elif isinstance(ui_res, list):
                                        for r in ui_res:
                                            if isinstance(r, dict) and (r.get('success') is True or r.get('status') == 'Success'):
                                                ui_ok = True
                                                break
                                    if ui_ok:
                                        return True
                            except Exception:
                                # swallow and continue with false return below
                                pass
                        return False
                    except Exception as e:
                        msg = str(e).lower()
                        # If retriable, try again
                        if attempt < TEST_RETRIES and ('429' in msg or 'toomanyrequests' in msg or 'timeout' in msg):
                            logger.warning(f"Transient error on test attempt {attempt + 1} for {label} ({ref_id}): {e}; retrying after backoff")
                            time.sleep(TEST_RETRY_DELAY_SEC * (attempt + 1))
                            continue
                        # non-retriable, return False
                        logger.debug(f"Non-retriable error during test for {label} ({ref_id}): {e}")
                        raise

            # 1) Try indexer as-is
            logger.info(f"Testing indexer as-is for {idx.get('name', idx_id)}")
            test_obj = copy.deepcopy(idx)
            ok = False
            try:
                ok = _perform_test_with_retries(client, test_obj, idx_id, f"as-is")
            except Exception as e:
                logger.debug(f"As-is test raised exception for {idx_id}: {e}")
            # If as-is full indexer test fails, try a UI-like minimal payload which
            # can follow a different server code path and may succeed where the
            # server-side test is different in the UI
            if not ok:
                ui_payload = build_ui_test_payload(test_obj)
                if ui_payload and ui_payload != test_obj:
                    try:
                        logger.info(f"Testing indexer with minimal UI-like payload for {idx.get('name', idx_id)}")
                        ok = _perform_test_with_retries(client, ui_payload, idx_id, f"as-is-ui")
                    except Exception as e:
                        logger.debug(f"As-is UI payload test raised exception for {idx_id}: {e}")
            if ok:
                logger.info(f"Index {idx.get('name', idx_id)} ({idx_id}) OK as-is; marking fixed (no update)")
                results['fixed'].append({'indexer': idx, 'new_base_url': None, 'tag': None})
                # clear any failure state for indexer
                if state_key in indexer_state:
                    try:
                        indexer_state.pop(state_key, None)
                        _save_indexer_state(indexer_state)
                    except Exception:
                        pass
                continue

            # 2) Try candidate base URLs
            for candidate in base_urls:
                logger.info(f"Testing candidate base URL {candidate}")
                # make a copy to test
                test_obj = copy.deepcopy(idx)
                set_base_url(test_obj, candidate)
                try:
                    ok = _perform_test_with_retries(client, test_obj, idx_id, candidate)
                    if ok:
                        logger.info(f"Candidate base URL {candidate} works; saving indexer")
                        if not DRY_RUN:
                            client.update_indexer(idx_id, test_obj)
                        results['fixed'].append({'indexer': idx, 'new_base_url': candidate, 'tag': tag_obj})
                        # clear any failure state for indexer
                        if state_key in indexer_state:
                            try:
                                indexer_state.pop(state_key, None)
                                _save_indexer_state(indexer_state)
                            except Exception:
                                pass
                        updated = True
                        break
                    else:
                        logger.warning(f"Candidate base URL {candidate} failed test, trying next")
                        # Try a UI-like minimal payload with the candidate set - may
                        # trigger a different server path
                        ui_candidate = build_ui_test_payload(test_obj)
                        if ui_candidate and ui_candidate != test_obj:
                            try:
                                logger.info(f"Testing candidate {candidate} with minimal UI-like payload")
                                ok = _perform_test_with_retries(client, ui_candidate, idx_id, f"{candidate}-ui")
                                if ok:
                                    logger.info(f"Candidate base URL {candidate} (UI payload) works; saving indexer")
                                    if not DRY_RUN:
                                        client.update_indexer(idx_id, ui_candidate)
                                    results['fixed'].append({'indexer': idx, 'new_base_url': candidate, 'tag': tag_obj})
                                    updated = True
                                    break
                                else:
                                    logger.warning(f"Candidate base URL (UI payload) {candidate} failed test")
                            except Exception as e:
                                logger.warning(f"Candidate UI payload {candidate} raised exception: {e}")
                except Exception as e:
                    logger.warning(f"Test for candidate base URL {candidate} raised exception: {e}")
                    if hasattr(e, 'args') and e.args:
                        try:
                            logger.debug(f"Detailed error: {str(e.args[0])}")
                        except Exception:
                            pass
                    try:
                        logger.debug(f"Test payload: {json.dumps(test_obj, default=str)[:4000]}")
                    except Exception:
                        pass
                    continue
                # If we have a tag configured, test again with the tag
                if TAG_TO_TRY:
                    logger.info(f"Trying candidate {candidate} again with tag {TAG_TO_TRY}")
                    test_obj_tag = copy.deepcopy(idx)
                    set_base_url(test_obj_tag, candidate)
                    # find or create tag
                    if not DRY_RUN:
                        tag_obj = client.find_or_create_tag(TAG_TO_TRY)
                    else:
                        tag_obj = {'id': -1, 'label': TAG_TO_TRY}
                    add_tag_to_indexer(test_obj_tag, tag_obj)
                    try:
                        # attempt test with retries for transient errors
                        test_res_tag = None
                        for attempt in range(0, TEST_RETRIES + 1):
                            try:
                                test_res_tag = client.test_indexer(test_obj_tag)
                                break
                            except Exception as e:
                                msg = str(e).lower()
                                if attempt < TEST_RETRIES and ('429' in msg or 'toomanyrequests' in msg or 'timeout' in msg):
                                    logger.warning(f"Transient error on test attempt {attempt + 1} (tag) for {candidate}: {e}; retrying after backoff")
                                    time.sleep(TEST_RETRY_DELAY_SEC * (attempt + 1))
                                    continue
                                else:
                                    raise
                        ok_tag = False
                        if isinstance(test_res_tag, dict):
                            ok_tag = test_res_tag.get('success') is True or test_res_tag.get('isSuccess') is True
                            if not ok_tag and any(v is True for v in test_res_tag.values() if isinstance(v, bool)):
                                ok_tag = True
                        elif isinstance(test_res_tag, list) and len(test_res_tag) > 0:
                            for r in test_res_tag:
                                if isinstance(r, dict) and (r.get('success') is True or r.get('status') == 'Success'):
                                    ok_tag = True
                                    break
                        if ok_tag:
                            logger.info(f"Candidate base URL {candidate} + tag {TAG_TO_TRY} works; saving indexer")
                            if not DRY_RUN:
                                client.update_indexer(idx_id, test_obj_tag)
                            results['fixed'].append({'indexer': idx, 'new_base_url': candidate, 'tag': tag_obj})
                            # clear any failure state for indexer
                            if state_key in indexer_state:
                                try:
                                    indexer_state.pop(state_key, None)
                                    _save_indexer_state(indexer_state)
                                except Exception:
                                    pass
                            updated = True
                            break
                        else:
                            logger.warning(f"Candidate base URL {candidate} + tag {TAG_TO_TRY} failed test")
                            # Try candidate+tag with a UI-like minimal payload
                            ui_candidate_tag = build_ui_test_payload(test_obj_tag)
                            if ui_candidate_tag and ui_candidate_tag != test_obj_tag:
                                try:
                                    logger.info(f"Testing candidate+tag {candidate} with minimal UI-like payload")
                                    ok_tag2 = _perform_test_with_retries(client, ui_candidate_tag, idx_id, f"{candidate}+tag-ui")
                                    if ok_tag2:
                                        logger.info(f"Candidate base URL {candidate} + tag (UI payload) {TAG_TO_TRY} works; saving indexer")
                                        if not DRY_RUN:
                                            client.update_indexer(idx_id, test_obj_tag)
                                            idx = client.get_indexer(idx_id)
                                        results['fixed'].append({'indexer': idx, 'new_base_url': candidate, 'tag': tag_obj})
                                        updated = True
                                        break
                                except Exception as e:
                                        logger.warning(f"Candidate base URL (UI payload) {candidate} + tag {TAG_TO_TRY} failed test")
                                except Exception as e:
                                    logger.warning(f"Candidate+tag UI payload {candidate} raised exception: {e}")
                            try:
                                logger.debug(f"Test response with tag: {test_res_tag}")
                            except Exception:
                                pass
                    except Exception as e:
                        logger.warning(f"Test with tag for candidate base URL {candidate} raised exception: {e}")
                        if hasattr(e, 'args') and e.args:
                            try:
                                logger.debug(f"Detailed error: {str(e.args[0])}")
                            except Exception:
                                pass
have the tag object
                    if tag_obj is None:
                        if not DRY_RUN:
                            try:
                                tag_obj = client.find_or_create_tag(TAG_TO_TRY)
                            except Exception as e:
                                logger.warning(f"Failed to find/create tag {TAG_TO_TRY}: {e}")
                                tag_obj = {'id': -1, 'label': TAG_TO_TRY}
                        else:
                            tag_obj = {'id': -1, 'label': TAG_TO_TRY}
                    # try original with tag
                    logger.info(f"Testing original indexer with tag {TAG_TO_TRY} for {idx.get('name', idx_id)}")
                    test_obj_tag = copy.deepcopy(idx)
                    add_tag_to_indexer(test_obj_tag, tag_obj)
                    try:
                        # Optionally persist the tag to Prowlarr so the server's 'test' uses the saved configuration
                        saved_original = None
                        if APPLY_TAG_SAVE_BEFORE_TEST and not DRY_RUN:
                            saved_original = copy.deepcopy(idx)
                            try:
                                add_tag_to_indexer(idx, tag_obj)
                                logger.info(f"Persisting tag {tag_obj} to indexer {idx_id} before testing")
                                resp = client.update_indexer(idx_id, idx)
                                logger.debug(f"Update response after applying tag: {resp}")
                                # fetch fresh copy from server and use it for testing
                                idx = client.get_indexer(idx_id)
                                test_obj_tag = copy.deepcopy(idx)
                            except Exception as e:
                                logger.warning(f"Failed to persist tag before testing for indexer {idx_id}: {e}")

                        ok = _perform_test_with_retries(client, test_obj_tag, idx_id, f"as-is+tag")
                        if not ok:
                            ui_test_obj_tag = build_ui_test_payload(test_obj_tag)
                            if ui_test_obj_tag and ui_test_obj_tag != test_obj_tag:
                                try:
                                    logger.info(f"Testing original indexer with minimal UI-like payload + tag {TAG_TO_TRY}")
                                    ok = _perform_test_with_retries(client, ui_test_obj_tag, idx_id, f"as-is+tag-ui")
                                except Exception as e:
                                    logger.debug(f"Original+tag UI payload test raised exception for {idx_id}: {e}")
                        if ok:
                            logger.info(f"Original indexer + tag {TAG_TO_TRY} works; saving indexer")
                            if not DRY_RUN:
                                client.update_indexer(idx_id, test_obj_tag)
                            results['fixed'].append({'indexer': idx, 'new_base_url': None, 'tag': tag_obj})
                            # clear any failure state for indexer
                            if state_key in indexer_state:
                                try:
                                    indexer_state.pop(state_key, None)
                                    _save_indexer_state(indexer_state)
                                except Exception:
                                    pass
                            updated = True
                    except Exception as e:
                        logger.warning(f"Test original+tag raised exception: {e}")
                        # If we saved the tag and test failed, revert
                        if APPLY_TAG_SAVE_BEFORE_TEST and not DRY_RUN and 'saved_original' in locals():
                            try:
                                logger.info(f"Reverting indexer {idx_id} to saved original after failed test")
                                resp = client.update_indexer(idx_id, saved_original)
                                logger.debug(f"Update response after revert: {resp}")
                                idx = client.get_indexer(idx_id)
                            except Exception as e2:
                                logger.warning(f"Failed to revert indexer after failed test for {idx_id}: {e2}")
                    # try each candidate with tag
                    if not updated:
                        for candidate in base_urls:
                            logger.info(f"Testing candidate base URL {candidate} + tag {TAG_TO_TRY}")
                            test_obj_tag = copy.deepcopy(idx)
                            set_base_url(test_obj_tag, candidate)
                            add_tag_to_indexer(test_obj_tag, tag_obj)
                            try:
                                if APPLY_TAG_SAVE_BEFORE_TEST and not DRY_RUN:
                                    try:
                                        saved_original = copy.deepcopy(idx)
                                        # persist tag + baseurl
                                        add_tag_to_indexer(idx, tag_obj)
                                        set_base_url(idx, candidate)
                                        logger.info(f"Persisting tag+baseUrl to indexer {idx_id} for candidate {candidate}")
                                        resp = client.update_indexer(idx_id, idx)
                                        logger.debug(f"Update response after applying tag+baseurl: {resp}")
                                        # refresh indexer from server
                                        idx = client.get_indexer(idx_id)
                                        test_obj_tag = copy.deepcopy(idx)
                                    except Exception as e:
                                        logger.warning(f"Failed to persist tag+baseurl before testing for indexer {idx_id}, candidate {candidate}: {e}")
                                ok = _perform_test_with_retries(client, test_obj_tag, idx_id, f"{candidate}+tag")
                                if ok:
                                    logger.info(f"Candidate base URL {candidate} + tag {TAG_TO_TRY} works; saving indexer")
                                    if not DRY_RUN:
                                        client.update_indexer(idx_id, test_obj_tag)
                                    results['fixed'].append({'indexer': idx, 'new_base_url': candidate, 'tag': tag_obj})
                                    updated = True
                                    break
                                else:
                                    logger.warning(f"Candidate base URL {candidate} + tag {TAG_TO_TRY} failed test")
                            except Exception as e:
                                logger.warning(f"Test for candidate+tag {candidate} raised exception: {e}")
                                if APPLY_TAG_SAVE_BEFORE_TEST and not DRY_RUN and 'saved_original' in locals():
                                    try:
                                        logger.info(f"Reverting indexer {idx_id} after candidate+tag failed test")
                                        resp = client.update_indexer(idx_id, saved_original)
                                        logger.debug(f"Update response after revert: {resp}")
                                        idx = client.get_indexer(idx_id)
                                    except Exception as e2:
                                        logger.warning(f"Failed to revert indexer after candidate+tag failed test for {idx_id}: {e2}")
                if not updated:
                    logger.info(f"No candidate base URL tested successfully for indexer {idx.get('name', idx_id)}; reverting")
                    # update indexer_state fail counters and cooldown
                    idx_state = indexer_state.get(state_key, {})
                    fail_count = idx_state.get('consecutive_failures', 0) + 1
                    if fail_count >= INDEXER_MAX_ATTEMPTS:
                        cooldown_until = now_ts + (INDEXER_COOLDOWN_MIN * 60)
                        idx_state['next_allowed_at'] = cooldown_until
                        idx_state['consecutive_failures'] = 0
                        logger.info(f"Indexer {idx.get('name', idx_id)} ({idx_id}) entering cooldown until {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(cooldown_until))} after {fail_count} failures")
                    else:
                        idx_state['consecutive_failures'] = fail_count
                        logger.debug(f"Indexer {idx.get('name', idx_id)} ({idx_id}) consecutive_failures set to {fail_count}")
                    indexer_state[state_key] = idx_state
                    try:
                        _save_indexer_state(indexer_state)
                    except Exception:
                        pass
                if not DRY_RUN:
                    client.update_indexer(idx_id, original)
                results['failed'].append(idx)
        except Exception as e:
            logger.exception(f"Unexpected exception while processing indexer: {e}")
            results['failed'].append(idx)

    return results


def run_loop():
    client = make_client()
    if client is None:
        logger.error('No Prowlarr client configured; cannot run loop')
        return
    while True:
        logger.info('Starting run cycle...')
        res = run_once(client)
        logger.info(f"Run complete: fixed={len(res['fixed'])} failed={len(res['failed'])} skipped={len(res['skipped'])}")
        logger.debug(res)
        logger.info(f"Sleeping {CHECK_INTERVAL_MIN} minutes...")
        time.sleep(CHECK_INTERVAL_MIN * 60)


if __name__ == '__main__':
    if ONE_SHOT:
        logger.info('ONE_SHOT enabled; running once and exiting')
        result = run_once()
        logger.info(f"Result: fixed={len(result['fixed'])} failed={len(result['failed'])} skipped={len(result['skipped'])}")
    else:
        run_loop()
