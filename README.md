# xstore
Convenience xarray accessor for writing xarray.DataArray and xarray.Dataset objects to disk, re-reading from disk and assigning to the original object. Helps with reducing the total number of dask tasks and associated overhead for long/intesive workflows.

Example usage:
```
import xarray as xr
import xstore as xst
import dask.array as da

xst.STORE_DIRECTORY = '/scratch/v14/ds0092/.xstore/'

da = xr.DataArray(da.random.random((10000, 10000), chunks=(1000, 1000)),
                  coords=[('a', range(10000)),
                          ('b', range(10000))]).rename('test')

# Do lots of operations to da building a very large dask graph

da = da.xst.store() # Write da to disc and re-read it

# Continue working with da
```

