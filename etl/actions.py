import numpy as np
import xarray as xr

from .actions_base import ActionExecutor


class XarrayDataSet(ActionExecutor):
    def _run_action(self, event):
        pass

    def _get_dataset(self):
        dependency_outputs = self.retrieve_dependencies_output()
        for output in dependency_outputs:
            if isinstance(output, xr.Dataset):
                return output

        raise ValueError("No xarray.Dataset was found in the dependency outputs")


class LoadToServer(XarrayDataSet):
    def _run_action(self, event, mapper, load_option: str = "file", *args, **kwargs):
        if load_option.lower() == "file":
            with mapper.fs.open(
                f"{mapper.root}/{event.src_path.rsplit('/', 1)[-1]}", mode="wb"
            ) as fa:
                with open(event.src_path, mode="rb") as fb:
                    fa.write(fb.read())
        elif load_option.lower() == "zarr":
            ds = self._get_dataset()
            tasks = ds.to_zarr(mapper, mode="a", compute=False, *args, **kwargs)
            tasks.compute()
        else:
            raise NotImplementedError(f"Engine '{load_option}' is not implemented.")

        return None


class OpenDataSet(XarrayDataSet):
    """
    Actions that opens a dataset using xarray.
    """

    def _run_action(self, event, engine: str = "netcdf4"):
        ds = xr.open_dataset(event.src_path, engine=engine).chunk("auto")
        return ds


class CleanDataSet(XarrayDataSet):
    """
    Actions that cleans a dataset using xarray.
    """

    def _run_action(self, event, engine: str = "netcdf4"):
        ds = self._get_dataset()
        supress_value = 1e20
        delta = 1e-6
        fill_val = np.nan

        a = ds["sos"].load()
        a.values = a.fillna(fill_val)
        a.values = xr.where(np.abs(a - supress_value) < delta, fill_val, a)
        a.encoding["_FillValue"] = fill_val

        return ds
