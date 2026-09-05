# uv + compression example

Writes the same checkpoint twice -- once with `compression="none"` (the
default), once with `compression="zstd"` set inline via a
`?compression=zstd` connection-string query parameter -- then prints each
checkpoint object's size on disk.

```bash
uv run main.py
```
