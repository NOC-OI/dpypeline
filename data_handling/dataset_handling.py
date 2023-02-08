import json

from kerchunk.hdf import SingleHdf5ToZarr


def generate_zarr_refs(
    paths,
    urls,
    fs_in,
    fs_out,
    fs_in_open_kwargs={},
    fs_out_open_kwargs={},
    refs_path="./",
    inline_threshold=300,
):
    """
    Translate the content of HDF5/NetCDF files into Zarr metadata.
    A single .json reference file is created for each file url passed to it.

    Parameters
    ----------
    paths: Iterable of strs
        Path(s) of the file(s) for which to create reference file(s).
    urls: Iterable of strs
        URL(s) of the file(s) for which reference file(s) will point to.
    fs_in: file-system instance
        Source filesystem (one of the compatible implementations in fsspec).
    fs_out: file-system instance
        Destination filesystem (one of the compatible implementations in fsspec).
    fs_in_open_kwargs: dict
        Arguments to pass to the fs_in.open function.
    fs_out_open_kwargs: dict
        Arguments to pass to the fs_out.open function.
    refs_path: str
        Path for the output references.
    inline_threshold: int
        Size below which binary blocks are included directly in the output.

    Returns
    -------
    refs: List of dicts
        References created.
    """
    refs = []

    for path, url in zip(paths, urls):
        with fs_in.open(path, **fs_in_open_kwargs) as infile:
            chunks = SingleHdf5ToZarr(infile, url, inline_threshold=inline_threshold)

            ref = chunks.translate()
            refs.append(ref)

            if refs_path is not None:
                output_file = (
                    f"{refs_path}{url.rsplit('.', 1)[0].rsplit('/', 1)[-1]}.json"
                )
                with fs_out.open(output_file, "wb", **fs_out_open_kwargs) as f:
                    f.write(json.dumps(ref).encode())

    return refs
