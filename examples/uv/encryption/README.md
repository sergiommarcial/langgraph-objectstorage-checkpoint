# uv + encryption example

Writes the same checkpoint twice -- once with `encryption=None` (the
default), once with a minimal `KeyProvider` -- then checks whether a known
plaintext value shows up in each checkpoint object's raw bytes on disk.

```bash
uv run main.py
```
