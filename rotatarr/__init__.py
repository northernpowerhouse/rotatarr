"""Package shim that exposes functions from the top-level rotatarr.py script
This allows unit tests to import `rotatarr` as a module while the main script
remains at repository root.
"""
import os
import importlib.util
import sys

_root = os.path.dirname(os.path.dirname(__file__))
_script_path = os.path.join(_root, 'rotatarr.py')
if os.path.exists(_script_path):
    spec = importlib.util.spec_from_file_location('rotatarr_script', _script_path)
    rotatarr_script = importlib.util.module_from_spec(spec)
    sys.modules['rotatarr_script'] = rotatarr_script
    spec.loader.exec_module(rotatarr_script)
    # expose known symbols
    for _name in ('get_alternate_base_urls', 'set_base_url', 'add_tag_to_indexer', 'is_indexer_error'):
        if hasattr(rotatarr_script, _name):
            globals()[_name] = getattr(rotatarr_script, _name)

__all__ = ['get_alternate_base_urls', 'set_base_url', 'add_tag_to_indexer', 'is_indexer_error']
