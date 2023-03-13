import glob

import xarray as xr

files = glob.glob(
    "/gws/nopw/j04/nemo_vol1/public/runs/ORCA0083-N06/means/**/ORCA0083-N06_*m*T.nc"
)

combine = ["by_coords", "nested"]
compat = ["identical", "equals", "broadcast_equals", "no_conflicts", "override"]
data_vars = ["minimal", "different", "all"]
data_coords = ["minimal", "different", "all"]
join = ["outer", "inner", "left", "right", "exact", "override"]
combine_attrs = ["drop", "identical", "no_conflicts", "drop_conflicts", "override"]


for comp in compat:
    for dv in data_vars:
        for dc in data_coords:
            for j in join:
                try:
                    ds = xr.open_mfdataset(
                        files,
                        combine="by_coords",
                        compat=comp,
                        data_vars=dv,
                        data_coords=dc,
                        join=j,
                        parallel=True,
                    )
                    print(comp, dv, dc, j)
                    print(ds)
                except:
                    pass
