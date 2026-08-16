"""Vulture whitelist: public API methods vulture can't see are used.

Vulture flags every public method of ObjectStorageSaver as "unused" since
nothing in this codebase calls them -- they're the library's public API,
called by consumers (and by LangGraph's Pregel loop) outside this repo.
Referencing them here (vulture's documented pattern) marks them as used
without disabling dead-code detection elsewhere, which --min-confidence
would do -- vulture reports unused functions/variables at the same 60%
confidence as these false positives, so raising the threshold hides real
dead code too.
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
