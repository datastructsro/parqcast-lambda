"""AWS Lambda entry point for parqcast export."""

import json
import logging
import os

logger = logging.getLogger(__name__)


def handler(event, context):
    """Lambda handler: run one tick of the parqcast export pipeline.

    Environment variables:
        DATABASE_URL: PostgreSQL connection string for the Odoo database
        TRANSPORT_TYPE: "s3" (default) or "http"
        # S3 transport
        S3_BUCKET: S3 bucket for parquet output
        S3_PREFIX: Optional prefix within bucket (default: "parqcast")
        S3_ENDPOINT_URL: Optional S3-compatible endpoint (MinIO, LocalStack)
        # HTTP transport
        SERVER_URL: Parqcast server URL
        API_KEY: API key for server auth
        NAMESPACE: Namespace for data organization
        # Common
        COMPANY: Company name
        COMPANY_ID: Company ID (integer)
        TIME_BUDGET: Export time budget in seconds (default: 270)
    """
    from parqcast.orchestrator import Orchestrator

    from .env import LambdaEnv

    dsn = os.environ["DATABASE_URL"]
    company = os.environ["COMPANY"]
    company_id = int(os.environ["COMPANY_ID"])
    time_budget = int(os.environ.get("TIME_BUDGET", "270"))
    transport_type = os.environ.get("TRANSPORT_TYPE", "s3")

    if transport_type == "s3":
        from parqcast.transport_s3 import S3Transport

        transport = S3Transport(
            bucket=os.environ["S3_BUCKET"],
            prefix=os.environ.get("S3_PREFIX", "parqcast"),
            endpoint_url=os.environ.get("S3_ENDPOINT_URL"),
            aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
            region_name=os.environ.get("AWS_REGION"),
        )
    elif transport_type == "http":
        from parqcast.transport_http import HttpTransport

        transport = HttpTransport(
            server_url=os.environ["SERVER_URL"],
            api_key=os.environ.get("API_KEY", ""),
            namespace=os.environ.get("NAMESPACE", "parqcast"),
        )
    else:
        raise ValueError(f"Unknown TRANSPORT_TYPE: {transport_type}")

    env = LambdaEnv(dsn)
    try:
        orch = Orchestrator(
            env=env,
            transport=transport,
            company=company,
            company_id=company_id,
            time_budget=time_budget,
        )
        result = orch.run()
        logger.info("Export tick: %s", result.get("state", "done"))
        return {
            "statusCode": 200,
            "body": json.dumps(result, default=str),
        }
    finally:
        env.close()
