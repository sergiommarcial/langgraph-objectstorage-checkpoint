"""Vulture whitelist: code vulture can't see is used.

Vulture flags every public method of ObjectStorageSaver as "unused" since
nothing in this codebase calls them -- they're the library's public API,
called by consumers (and by LangGraph's Pregel loop) outside this repo.
`_bg_thread` is a different case of the same problem: it's read by
tests/unit/test_saver_core.py's background-loop tests, but vulture only
scans `src` and this file, so it can't see that use either. Referencing
these here (vulture's documented pattern) marks them as used without
disabling dead-code detection elsewhere, which --min-confidence would do
-- vulture reports unused functions/variables at the same 60% confidence
as these false positives, so raising the threshold hides real dead code
too.
"""

from langgraph_checkpoint_objectstorage.saver import ObjectStorageSaver

ObjectStorageSaver.from_conn_string
ObjectStorageSaver.get_tuple
ObjectStorageSaver.aget_tuple
ObjectStorageSaver.list
ObjectStorageSaver.alist
ObjectStorageSaver.put
ObjectStorageSaver.aput
ObjectStorageSaver.put_writes
ObjectStorageSaver.aput_writes
ObjectStorageSaver.delete_thread
ObjectStorageSaver.adelete_thread
ObjectStorageSaver.delete_expired
ObjectStorageSaver.adelete_expired
ObjectStorageSaver._bg_thread
