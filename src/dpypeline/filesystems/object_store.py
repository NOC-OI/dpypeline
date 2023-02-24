"""S3 object store class."""
import json
import logging
import os

import fsspec
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
                f"Object store credentials will be read from the JSON file '{store_credentials_json}'"
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
            Absolute or relative filepath to the JSON file containing the object store credentials.

        Returns
        -------
        store_credentials
            Dictionary containing the values of the `token`, `secret` and `endpoint_url` keys used
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
                    f'"{key}" is not a key in the JSON file provided. Its value will be set to None.'
                )

        return store_credentials

    def get_remote_options(self, override: bool = False) -> dict:
        """
        Get the remote options of the object store.

        Parameters
        ----------
        override
            Flag to create remote_options from scratch (True) or to simply retrieve the current dict (False).

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
        """
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

    def write_file_to_bucket(self, path: str, bucket: str) -> None:
        """
        Write file from the local filesystem to a bucket of the object store.

        Parameters
        ----------
        path
            Absolute or relative filepath of the file to be written to the object store.
        bucket
            Name of the bucket to place the file in.
        """
        assert bucket in self.get_bucket_list(), f'Bucket "{bucket}" does not exist.'
        assert os.path.isfile(path), f'"{path}" is not a file.'

        with self.open(
            f"{bucket}/{path.rsplit('/', 1)[-1]}", mode="wb", s3=dict(profile="default")
        ) as fa:
            with open(path, mode="rb") as fb:
                fa.write(fb.read())
