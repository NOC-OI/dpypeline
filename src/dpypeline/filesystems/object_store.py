"""S3 object store class."""
import json
import logging
import os

import fsspec
import numpy as np
import s3fs


class ObjectStoreS3(s3fs.S3FileSystem):
    """
    S3 object store.

    Parameters
    ----------
    s3fs
        _description_
    """

    def __init__(
        self,
        anon: bool = False,
        store_credentials_json: str | None = None,
        secret: str | None = None,
        key: str | None = None,
        endpoint_url: str | None = None,
        *fs_args,
        **fs_kwargs,
    ) -> None:
        """
        Initialize the S3 object store.

        Parameters
        ----------
        anon, optional
            _description_, by default False
        store_credentials_json, optional
            _description_, by default None
        secret, optional
            _description_, by default None
        key, optional
            _description_, by default None
        endpoint_url, optional
            _description_, by default None
        """
        self._anon = anon
        logging.info("-" * 79)
        if store_credentials_json is None:
            logging.info(
                "No JSON file was provided."
                "Object store credentials will be obtained from the arguments passed."
            )
            self._store_credentials = {
                "secret": secret,
                "token": key,
                "endpoint_url": endpoint_url,
            }
        else:
            logging.info(
                "Object store credentials will be read from the JSON file "
                + f"{store_credentials_json}"
            )
            self._store_credentials = self.load_store_credentials(
                store_credentials_json
            )

        self._remote_options = self.get_remote_options(override=True)

        super().__init__(*fs_args, **self._remote_options, **fs_kwargs)

    @staticmethod
    def load_store_credentials(path: str) -> dict:
        """
        Set the credentials of the object store from a JSON file.

        Parameters
        ----------
        path
            Absolute or relative filepath to the JSON file containing
            the object store credentials.

        Returns
        -------
        store_credentials
            Dictionary containing the values of the `token`,
                `secret` and `endpoint_url` keys used
            to access the object store.
        """
        try:
            with open(path) as f:
                store_credentials = json.load(f)
        except Exception as error:
            raise Exception(error)

        for key in ["token", "secret", "endpoint_url"]:
            if key not in store_credentials:
                logging.info("-" * 79)
                logging.warning(
                    f'"{key}" is not a key in the JSON file provided. '
                    + "Its value will be set to None."
                )

        return store_credentials

    def create_bucket(self, bucket: str, **kwargs) -> None:
        """
        Create a bucket in the object store.

        Parameters
        ----------
        bucket
            Bucket to create.
        """
        try:
            return self.mkdir(bucket, **kwargs)
        except FileExistsError:
            logging.info(f"Bucket '{bucket}' already exists.")

    def get_remote_options(self, override: bool = False) -> dict:
        """
        Get the remote options of the object store.

        Parameters
        ----------
        override
            Flag to create remote_options from scratch (True)
            or to simply retrieve the current dict (False).

        Returns
        -------
        remote_options
            Dictionary containing the remote options of the object store.

        """
        if override:
            self._remote_options = {
                "anon": self._anon,
                "secret": self._store_credentials["secret"],
                "key": self._store_credentials["token"],
                "client_kwargs": {
                    "endpoint_url": self._store_credentials["endpoint_url"]
                },
            }

        return self._remote_options

    def get_mapper(
        self, bucket: str, prefix: str = "s3://", **get_mapper_kwargs
    ) -> fsspec.mapping.FSMap:
        """
        Make a MutableMaping interface to the desired bucket.

        Parameters
        ----------
        bucket
            Name of the bucket to place the file in.
        prefix: str, default "s3://"
            Protocol prefix
        **get_mapper_kwargs
            Kwargs for get_mapper. See: https://filesystem-spec.readthedocs.io/en/latest/api.html#fsspec.get_mapper.

        Returns
        -------
        mapper
            Dict-like key-value store.
        """  # noqa adamwa
        mapper = fsspec.get_mapper(
            prefix + bucket, **self._remote_options, **get_mapper_kwargs
        )

        return mapper

    def get_bucket_list(self) -> list[str]:
        """
        Get the list of buckets in the object store.

        Returns
        -------
        bucket_list
            List of the object store buckets.
        """
        return self.ls("/")

    def write_file_to_bucket(
        self, path: str, bucket: str, chunk_size: int = -1, parallel: bool = False
    ) -> None:
        """
        Write file from the local filesystem to a bucket of the object store.

        Parameters
        ----------
        path
            Absolute or relative filepath of the file to be written to the object store.
        bucket
            Name of the bucket to place the file in.
        chunk_size
            Size of the chunk (in bytes) to read/write at once.
            If the chunk size is -1, the file will be read/written at once.
        parallel
            Flag to enable parallel writing (True) or not (False).
        """
        assert (
            bucket.split(os.path.sep, 1)[0] in self.get_bucket_list()
        ), f'Bucket "{bucket}" does not exist.'
        assert os.path.isfile(path), f'"{path}" is not a file.'

        dest_path = os.path.join(bucket, path.rsplit(os.path.sep, 1)[-1])

        # Create dictionary of chunk offsets and lengths
        file_size = os.path.getsize(path)
        chks = self._create_chunks_offsets_lengths(file_size, chunk_size)

        if parallel:
            self._write_file_to_bucket_parallel(path, dest_path, chks)
        else:
            self._write_file_to_bucket_serial(path, dest_path, chks)

    def _write_file_to_bucket_parallel(
        self, path: str, dest_path: str, chunks_offsets_lengths: dict
    ) -> None:
        """
        Write file from the local filesystem to a bucket of the object store in parallel.

        Parameters
        ----------
        path
            Absolute or relative filepath of the file to be written to the object store.
        dest_path
            Path of the file in the object store.
        chunks_offsets_lengths
            Dictionary containing the chunk offsets and lengths.
        """
        import dask

        batches = []
        for chk in chunks_offsets_lengths.values():
            result_batch = dask.delayed(self._write_chunk)(
                path, dest_path, chk["offset"], chk["length"]
            )
            batches.append(result_batch)

        dask.compute(batches)

    def _write_file_to_bucket_serial(
        self, path: str, dest_path: str, chunks_offsets_lengths: dict
    ) -> None:
        """
        Write file from the local filesystem to a bucket of the object store in serial.

        Parameters
        ----------
        path
            Absolute or relative filepath of the file to be written to the object store.
        dest_path
            Path of the file in the object store.
        chunks_offsets_lengths
            Dictionary containing the chunk offsets and lengths.
        """
        # Write the file
        for chk in chunks_offsets_lengths.values():
            self._write_chunk(path, dest_path, chk["offset"], chk["length"])

    def _write_chunk(
        self, path_read, path_write, chunk_offset: int, chunk_length: int
    ) -> None:
        """
        Write a chunk to an open file.

        Parameters
        ----------
        path_read
            File to read from.
        path_write
            File to write to.
        chunk_offset
            Offset of the chunk (in bytes).
        chunk_length
            Length of the chunk (in bytes).
        """
        # Read the chunk
        with open(path_read, mode="rb") as f:
            f.seek(chunk_offset, 0)
            bytechunk = f.read(chunk_length)

        # Write the chunk
        with self.open(path_write, mode="rb+", s3=dict(profile="default")) as f:
            f.seek(chunk_offset, 0)
            f.write(bytechunk)

    def _create_chunks_offsets_lengths(self, nbytes, chunk_size: int) -> dict:
        """
        Create the chunks offsets and lengths for a file with nbytes given the chunk_size.

        Parameters
        ----------
        nbytes
            Number of bytes of the file.
        chunk_size
            Size of the chunk (in bytes).

        Returns
        -------
        chunks_offsets_lengths
            Dictionary containing the chunks offsets and lengths.
        """
        chunks_offsets_lengths = {}

        # Calculate the number of chunks
        nchunks = int(np.ceil(nbytes / chunk_size))

        # Calculate the chunks offsets and length
        for i in range(nchunks):
            chk_id = f"chunk_{i}"
            chunks_offsets_lengths[chk_id] = {
                "offset": i * chunk_size,
                "length": chunk_size,
            }

        return chunks_offsets_lengths
