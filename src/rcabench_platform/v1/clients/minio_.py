import minio

type MinioClient = minio.Minio


def get_minio_client() -> MinioClient:
    client = minio.Minio(
        endpoint="10.10.10.38:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False,
    )
    return client
