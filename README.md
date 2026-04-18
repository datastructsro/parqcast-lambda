# parqcast-lambda

AWS Lambda handler and S3 transport for [parqcast](https://github.com/datastructsro/parqcast).

Runs the parqcast export pipeline as a Lambda function, connecting directly to an Odoo PostgreSQL database and uploading Parquet files to S3.

## Packages

| Package | Description |
|---|---|
| `parqcast-lambda` | Lambda entry point, direct psycopg2 adapter |
| `parqcast-transport-s3` | S3/MinIO/LocalStack upload transport |

## Dependencies

`parqcast-collectors` and `parqcast-core` (transitively) are not published to PyPI. Install from the latest `main` of [datastructsro/parqcast](https://github.com/datastructsro/parqcast):

```bash
pip install "parqcast-collectors @ git+https://github.com/datastructsro/parqcast.git@main#subdirectory=packages/parqcast-collectors"
```

Or pin it via `[tool.uv.sources]` in your own `pyproject.toml`:

```toml
[tool.uv.sources]
parqcast-collectors = { git = "https://github.com/datastructsro/parqcast.git", subdirectory = "packages/parqcast-collectors", branch = "main" }
```

## Quick start

### Deploy as Lambda

1. Set environment variables on the Lambda function:

   | Variable | Required | Description |
   |---|---|---|
   | `DATABASE_URL` | yes | PostgreSQL connection string |
   | `COMPANY` | yes | Odoo company name |
   | `COMPANY_ID` | yes | Odoo company ID |
   | `S3_BUCKET` | yes | Target S3 bucket |
   | `S3_PREFIX` | no | Key prefix (default: `parqcast`) |
   | `S3_ENDPOINT_URL` | no | For S3-compatible stores |
   | `TIME_BUDGET` | no | Seconds per tick (default: `270`) |

2. Set the handler to `parqcast.lambda_handler.handler`.

### Local development with SAM

```bash
cd demo-lambda
sam build && sam local invoke -e event.json
```

### Using the S3 transport standalone

```python
from parqcast.transport_s3 import S3Transport

transport = S3Transport(
    bucket="my-bucket",
    prefix="parqcast",
    endpoint_url="http://localhost:9000",  # MinIO
)
transport.upload_file("run-uuid", "products.parquet", file_obj)
```

## About

Built and maintained by **[DataStruct s.r.o.](https://datastruct.tech)** — an **[Odoo Official Partner](https://www.odoo.com/partners)** based in the Czech Republic, specialising in demand forecasting and ERP implementation for manufacturing, retail, and logistics in the Czech–German–Polish triangle.

- **See it in production:** [SAP→Odoo migration, 84% cost reduction](https://datastruct.tech/post/sap-to-odoo-vibecoded-migration)
- **Book a 30-min technical diagnostic:** [cal.com/oleg-popov-sjwko9/30min](https://cal.com/oleg-popov-sjwko9/30min)
- **Questions / partnership:** info@datastruct.tech
- **Companion:** [parqcast](https://github.com/datastructsro/parqcast) (the exporter this runs) · [foreqcast](https://github.com/datastructsro/foreqcast) · [parqcast-server](https://github.com/datastructsro/parqcast-server)

## License

Apache-2.0
