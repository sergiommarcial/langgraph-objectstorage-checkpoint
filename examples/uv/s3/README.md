# uv + S3 example (sequential sessions + resume)

Multiple independent sessions (thread_ids) run one after another, then one
is resumed later from where it left off. See
[`../s3-async`](../s3-async) for the concurrent/async version, and why
it's a separate example rather than a second code path in this one.

Against real AWS S3: edit `main.py`'s `"s3://your-bucket/checkpoints"` to
a real bucket you have access to, make sure your AWS credentials are
configured (env vars, `~/.aws/credentials`, instance/task role), then:

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
