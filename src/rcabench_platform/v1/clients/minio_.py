import minio


def get_minio_client() -> minio.Minio:
    client = minio.Minio(
        endpoint="10.10.10.38:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False,
    )
    return client
