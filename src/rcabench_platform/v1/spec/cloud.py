from ..utils.fmap import fmap_threadpool
from ..logging import timeit

from collections.abc import Generator
from contextlib import contextmanager
from abc import ABC, abstractmethod
from pathlib import Path
import subprocess
import functools
import tempfile
import shutil

from huggingface_hub import HfApi
import minio


class Storage(ABC):
    @abstractmethod
    def upload_meta(self, dataset: str) -> None: ...

    @abstractmethod
    def upload_datapack(self, dataset: str, datapack: str) -> None: ...

    @abstractmethod
    def upload_dataset(self, dataset: str) -> None: ...

    @abstractmethod
    def download_meta(self, dataset: str) -> None: ...

    @abstractmethod
    def download_datapack(self, dataset: str, datapack: str) -> None: ...

    @abstractmethod
    def download_dataset(self, dataset: str) -> None: ...


@contextmanager
def zipped_folder(folder_path: Path, zip_name: str) -> Generator[tuple[Path, Path], None, None]:
    assert folder_path.is_dir()
    assert folder_path.parent.is_dir()

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        zip_path = tmp_path / f"{zip_name}.zip"

        subprocess.run(
            ["zip", "-r", str(zip_path), str(folder_path.name)],
            cwd=folder_path.parent,
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        sha256_path = zip_path.with_suffix(".sha256")
        subprocess.run(
            ["sha256sum", str(zip_path.name)],
            cwd=tmp_path,
            check=True,
            stdout=sha256_path.open("w"),
            stderr=subprocess.DEVNULL,
        )

        yield zip_path, sha256_path


@contextmanager
def unzipped_folder(zip_path: Path) -> Generator[Path, None, None]:
    assert zip_path.is_file()

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        subprocess.run(
            ["unzip", str(zip_path)],
            cwd=str(tmpdir),
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        folder = tmpdir / zip_path.stem
        assert folder.is_dir()

        yield folder


class MinioStorage(Storage):
    def __init__(
        self,
        *,
        local_root: Path,
        minio_client: minio.Minio,
        bucket: str,
        object_root: str,
        concurrent_upload: int = 0,
        concurrent_download: int = 0,
    ) -> None:
        assert local_root.is_dir()
        self._local_root = local_root

        assert minio_client.bucket_exists(bucket)
        self._minio = minio_client
        self._bucket = bucket
        self._object_root = object_root

        self._concurrent_upload = concurrent_upload
        self._concurrent_download = concurrent_download

    def _get_meta_key(self, dataset: str) -> str:
        key = f"__meta__/{dataset}"
        if self._object_root:
            key = f"{self._object_root}/{key}"
        return key

    def _get_datapack_key(self, dataset: str, datapack: str) -> str:
        key = f"{dataset}/{datapack}"
        if self._object_root:
            key = f"{self._object_root}/{key}"
        return key

    def _get_meta_path(self, dataset: str) -> Path:
        return self._local_root / "__meta__" / dataset

    def _get_datapack_path(self, dataset: str, datapack: str) -> Path:
        return self._local_root / dataset / datapack

    def _upload_folder(self, folder_path: Path, object_key: str):
        with zipped_folder(folder_path, folder_path.name) as (zip_path, sha256_path):
            self._minio.fput_object(
                bucket_name=self._bucket,
                object_name=object_key + ".zip",
                file_path=str(zip_path),
            )
            self._minio.fput_object(
                bucket_name=self._bucket,
                object_name=object_key + ".sha256",
                file_path=str(sha256_path),
            )

    def _download_folder(self, folder_path: Path, object_key: str):
        folder_path.parent.mkdir(parents=True, exist_ok=True)
        assert not folder_path.exists()

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            zip_path = tmpdir / f"{folder_path.name}.zip"
            sha256_path = tmpdir / f"{folder_path.name}.sha256"

            self._minio.fget_object(
                bucket_name=self._bucket,
                object_name=object_key + ".zip",
                file_path=str(zip_path),
            )
            self._minio.fget_object(
                bucket_name=self._bucket,
                object_name=object_key + ".sha256",
                file_path=str(sha256_path),
            )

            assert zip_path.is_file()
            assert sha256_path.is_file()

            subprocess.run(
                ["sha256sum", "-c", str(sha256_path.name)],
                cwd=tmpdir,
                check=True,
                # stdout=subprocess.DEVNULL,
                # stderr=subprocess.DEVNULL,
            )

            with unzipped_folder(zip_path) as zip_folder:
                shutil.move(zip_folder, folder_path)

    @timeit()
    def upload_meta(self, dataset: str) -> None:
        meta_folder = self._get_meta_path(dataset)
        meta_key = self._get_meta_key(dataset)
        self._upload_folder(meta_folder, meta_key)

    @timeit()
    def upload_datapack(self, dataset: str, datapack: str) -> None:
        datapack_folder = self._get_datapack_path(dataset, datapack)
        datapack_key = self._get_datapack_key(dataset, datapack)
        self._upload_folder(datapack_folder, datapack_key)

    @timeit()
    def upload_dataset(self, dataset: str) -> None:
        dataset_path = self._local_root / dataset
        assert dataset_path.is_dir()

        tasks = []
        for datapack_path in dataset_path.iterdir():
            assert datapack_path.is_dir()
            datapack = datapack_path.name
            tasks.append(functools.partial(self.upload_datapack, dataset, datapack))

        fmap_threadpool(tasks, parallel=self._concurrent_upload)

        self.upload_meta(dataset)

    @timeit()
    def download_meta(self, dataset: str) -> None:
        meta_folder = self._get_meta_path(dataset)
        meta_key = self._get_meta_key(dataset)
        self._download_folder(meta_folder, meta_key)

    @timeit()
    def download_datapack(self, dataset: str, datapack: str) -> None:
        datapack_folder = self._get_datapack_path(dataset, datapack)
        datapack_key = self._get_datapack_key(dataset, datapack)
        self._download_folder(datapack_folder, datapack_key)

    @timeit()
    def download_dataset(self, dataset: str) -> None:
        if self._object_root:
            prefix = f"{self._object_root}/{dataset}/"
        else:
            prefix = f"{dataset}/"

        datapacks = []
        for object in self._minio.list_objects(self._bucket, prefix=prefix):
            key = object.object_name
            assert isinstance(key, str)

            assert key.startswith(prefix)

            if not key.endswith(".sha256"):
                continue

            datapack = key.removeprefix(prefix).removesuffix(".sha256")
            datapacks.append(datapack)

        dataset_path = self._local_root / dataset
        dataset_path.mkdir(exist_ok=True)

        tasks = []
        for datapack in datapacks:
            tasks.append(functools.partial(self.download_datapack, dataset, datapack))

        fmap_threadpool(tasks, parallel=self._concurrent_download)

        self.download_meta(dataset)


class HuggingFaceStorage(Storage):
    def __init__(
        self,
        local_root: Path,
        hf_client: HfApi,
        repo_id: str,
        concurrent_upload: int = 0,
        concurrent_download: int = 0,
    ) -> None:
        assert local_root.is_dir()
        self._local_root = local_root

        self._hf = hf_client
        self._repo_id = repo_id

        self._concurrent_upload = concurrent_upload
        self._concurrent_download = concurrent_download

    def _upload_folder(self, folder_name: Path, commit_message: str) -> None:
        folder_path = self._local_root / folder_name
        assert folder_path.is_dir()

        self._hf.upload_folder(
            repo_id=self._repo_id,
            repo_type="dataset",
            path_in_repo=str(folder_name),
            folder_path=str(folder_path),
            commit_message=commit_message,
        )

    def _download_folder(self, folder_name: Path) -> None:
        folder_path = self._local_root / folder_name
        assert not folder_path.exists()

        self._hf.snapshot_download(
            repo_id=self._repo_id,
            repo_type="dataset",
            local_dir=self._local_root,
            allow_patterns=[str(f"{folder_name}/*")],
            max_workers=self._concurrent_download,
        )

    @timeit()
    def upload_meta(self, dataset: str) -> None:
        self._upload_folder(
            folder_name=Path("__meta__") / dataset,
            commit_message=f"upload meta `{dataset}`",
        )

    @timeit()
    def upload_datapack(self, dataset: str, datapack: str) -> None:
        self._upload_folder(
            folder_name=Path(dataset) / datapack,
            commit_message=f"upload datapack `{dataset}/{datapack}`",
        )

    @timeit()
    def upload_dataset(self, dataset: str) -> None:
        dataset_path = self._local_root / dataset
        assert dataset_path.is_dir()

        tasks = []
        for datapack_path in dataset_path.iterdir():
            assert datapack_path.is_dir()
            datapack = datapack_path.name
            tasks.append(functools.partial(self.upload_datapack, dataset, datapack))

        fmap_threadpool(tasks, parallel=self._concurrent_upload)

        self.upload_meta(dataset)

    @timeit()
    def download_meta(self, dataset: str) -> None:
        self._download_folder(folder_name=Path("__meta__") / dataset)

    @timeit()
    def download_datapack(self, dataset: str, datapack: str) -> None:
        self._download_folder(folder_name=Path(dataset) / datapack)

    @timeit()
    def download_dataset(self, dataset: str) -> None:
        dataset_path = self._local_root / dataset
        dataset_path.mkdir(exist_ok=True)

        self._hf.snapshot_download(
            repo_id=self._repo_id,
            repo_type="dataset",
            local_dir=self._local_root,
            allow_patterns=[f"{dataset}/*"],
            max_workers=self._concurrent_download,
        )

        self.download_meta(dataset)
