# poetry + GCS example (async, concurrent sessions)

See [`../gcs`](../gcs) for the sequential/resume version, and why this is
a separate example rather than a second code path in that one.

Against real Google Cloud Storage: edit `main.py`'s `"gcs://your-bucket/checkpoints"`
to a real bucket you have access to, make sure
[Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials)
are set up, then:

```bash
poetry install
poetry run python main.py
```

Against a local emulator instead (no GCP account needed): start
`fake-gcs-server` from the repo root (`docker compose up -d`), then:

```bash
poetry install
STORAGE_EMULATOR_HOST=http://127.0.0.1:4443 poetry run python main.py
```
