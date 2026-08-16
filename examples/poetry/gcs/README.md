# poetry + GCS example (sequential sessions + resume)

Multiple independent sessions (thread_ids) run one after another, then one
is resumed later from where it left off. See
[`../gcs-async`](../gcs-async) for the concurrent/async version, and why
it's a separate example rather than a second code path in this one.

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
