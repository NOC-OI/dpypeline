import os
import logging
import s3fs
import json
import fsspec


class ObjectStoreS3(s3fs.S3FileSystem):
    """
    TODO: write docstrings
    """
    def __init__(self, anon=False, store_credentials_json=None, secret=None, key=None, endpoint_url=None,
                 *fs_args, **fs_kwargs):
        self._anon = anon

        if store_credentials_json is None:
            logging.info("No JSON file was provided."
                         "Object store credentials will be obtained from the arguments passed.")
            self._store_credentials = {
                "secret": secret,
                "token": key,
                "endpoint_url": endpoint_url,
            }
        else:
            logging.info(f"Object store credentials will be read from the JSON file '{store_credentials_json}'")
            self._store_credentials = self.load_store_credentials(store_credentials_json)

        self._remote_options = self.get_remote_options(override=True)

        super().__init__(*fs_args,
                         **self._remote_options,
                         **fs_kwargs)

    @staticmethod
    def load_store_credentials(path):
        """
        Set the credentials of the object store from a JSON file.

        Parameters
        ----------
        path: str
            Absolute or relative filepath to the JSON file containing the object store credentials.

        Returns
        -------
        store_credentials: dict
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
                logging.warning(f"\"{key}\" is not a key in the JSON file provided. Its value will be set to None.")

        return store_credentials

    def get_remote_options(self, override=False):
        """
        Get the remote options of the object store.

        Returns
        -------
        remote_options: dict
            Dictionary containing the remote options of the object store.

        """
        if override:
            self._remote_options = {"anon": self._anon,
                                    "secret": self._store_credentials["secret"],
                                    "key": self._store_credentials["token"],
                                    "client_kwargs": {"endpoint_url": self._store_credentials["endpoint_url"]}}

        return self._remote_options

    def get_mapper(self, bucket, prefix="s3://", **get_mapper_kwargs):
        """
        Make a MutableMaping interface to the desired bucket.

        Parameters
        ----------
        bucket: str
            Name of the bucket to place the file in.
        prefix: str, default "s3://"
            Protocol prefix
        **get_mapper_kwargs: dict
            Kwargs for get_mapper. See: https://filesystem-spec.readthedocs.io/en/latest/api.html#fsspec.get_mapper.

        Returns
        -------
        mapper: `fsspec.mapping.FSMap` instance
            Dict-like key-value store.
        """
        mapper = fsspec.get_mapper(prefix + bucket,
                                   **self._remote_options,
                                   **get_mapper_kwargs)

        return mapper

    def get_bucket_list(self):
        """
        Get the list of buckets in the object store.

        Returns
        -------
        bucket_list: list of str
            List of the object store buckets.
        """
        return self.ls("/")

    def write_file_to_bucket(self, path, bucket):
        """
        Write file from the local filesystem to a bucket of the object store.

        Parameters
        ----------
        path: str
            Absolute or relative filepath of the file to be written to the object store.
        bucket: str
            Name of the bucket to place the file in.
        """
        assert bucket in self.get_bucket_list(), f"Bucket \"{bucket}\" does not exist."
        assert os.path.isfile(path), f"\"{path}\" is not a file."

        with self.open(f"{bucket}/{path.rsplit('/', 1)[-1]}", mode='wb', s3=dict(profile='default')) as fa:
            with open(path, mode="rb") as fb:
                fa.write(fb.read())
