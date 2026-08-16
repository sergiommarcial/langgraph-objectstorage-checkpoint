# uv + S3 example (async, concurrent sessions)

See [`../s3`](../s3) for the sequential/resume version, and why this is a
separate example rather than a second code path in that one.

Against real AWS S3: edit `main.py`'s `"s3://your-bucket/checkpoints"` to
a real bucket you have access to, make sure your AWS credentials are
configured, then:

```bash
uv run main.py
```

Against a local emulator instead (no AWS account needed): start
`moto-server` from the repo root (`docker compose up -d`), then:

```bash
AWS_ENDPOINT_URL=http://127.0.0.1:5001 uv run main.py
```

If the `test-checkpoints` bucket doesn't already exist on the emulator
(`make test-integration` creates it, so it usually does), create it first:

```bash
aws --endpoint-url=http://127.0.0.1:5001 s3 mb s3://test-checkpoints
```
