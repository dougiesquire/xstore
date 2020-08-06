# xcache
Convenience xarray accessor for writing objects to disk, re-reading from disk and assigning to the original object. Helps with reducing the total number of dask tasks and associated overhead.

Example usage:
```
import xarray as xr
import xcache as xc

da = xr.DataArray(np.random.normal(size=(180,360)),             
                  coords=[('lat', np.arange(-90,90,1)),
                          ('lon', np.arange(-280,80,1))]).rename('test')
da = da.xc.cache()

ds = da.to_dataset()
ds = ds.xc.cache()
```

