# xcache
Convenience xarray accessor for writing xarray.DataArray and xarray.Dataset objects to disk, re-reading from disk and assigning to the original object. Helps with reducing the total number of dask tasks and associated overhead for long/intesive workflows.

Example usage:
```
import xarray as xr
import xcache as xc

xc.CACHE_DIRECTORY = '/scratch/v14/ds0092/.xcache/'

da = xr.DataArray(np.random.normal(size=(180,360)),             
                  coords=[('lat', np.arange(-90,90,1)),
                          ('lon', np.arange(-280,80,1))]).rename('test')

# Do lots of operations to da building a very large dask graph

da = da.xc.cache() # Write da to disc and re-read it

# Continue working with da
```

