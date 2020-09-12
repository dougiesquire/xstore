import os
import uuid
import shutil
import zipfile
import xarray as xr

CACHE_DIRECTORY = '/scratch/v14/ds0092/.xcache/'


def _zip_zarr(zarr_DS, delete_DS=True):
    """ 
        Zip a zarr DirectoryStore.
    """
    filename = zarr_DS+os.path.extsep+"zip"
    with zipfile.ZipFile(filename, "w", compression=zipfile.ZIP_STORED, allowZip64=True) as fh:
        for root, _, filenames in os.walk(zarr_DS):
            for each_filename in filenames:
                each_filename = os.path.join(root, each_filename)
                fh.write(
                    each_filename,
                    os.path.relpath(each_filename, zarr_DS))            
    if delete_DS:
        shutil.rmtree(zarr_DS)
                
                
def _cache(obj, file_name=None, clobber=False, file_format='zarr_DS'):
    """
        Write an xarray object to disk and read it back
        
        Parameters
        ----------
        obj : xarray DataArray or Dataset
            data to cache and read back
        file_name : str, optional
            Name of file to write to disk. If not given, a random name will be generated
        clobber : boolean, optional
            If True, replace file if it already exists
        file_format : string, optional
            file format of the cached data. Options are 'zarr_DS' (zarr DirectoryStore), \
            'zarr_ZS' (zarr ZipStore), 'netcdf'

        Returns
        -------
        xarray DataArray or Dataset
            Same data as input, but now read directly from disk
    """
    
    os.makedirs(CACHE_DIRECTORY, exist_ok=True)
    
    for var in obj.variables:
        obj[var].encoding = {}
    
    if file_name is None:
        file_name = uuid.uuid4().hex
    if (file_format == 'zarr_DS') & (not file_name.endswith(os.path.extsep+'zarr')):
        file_name = file_name+os.path.extsep+'zarr'
    elif (file_format == 'zarr_ZS') & (not file_name.endswith(os.path.extsep+'zip')):
        file_name = file_name+os.path.extsep+'zip'
    elif (file_format == 'netcdf') & (not file_name.endswith(os.path.extsep+'nc')):
        file_name = file_name+os.path.extsep+'nc'
        
    cache_file = os.path.join(CACHE_DIRECTORY, file_name)    

    if file_format == 'zarr_DS':
        if (not os.path.exists(cache_file)) | clobber==True:
            obj.to_zarr(cache_file, mode='w', consolidated=True, compute=True)
        return xr.open_zarr(cache_file, consolidated=True)
    elif file_format == 'zarr_ZS':
        if (os.path.splitext(cache_file)[-1] != os.path.extsep+'zip'):
            tmp_file = cache_file
            cache_file = cache_file+os.path.extsep+'zip'
        else:
            tmp_file = cache_file.strip(os.path.extsep+'zip')
        if (not os.path.exists(cache_file)) | clobber==True:
            obj.to_zarr(tmp_file, mode='w', consolidated=True, compute=True)
            _zip_zarr(tmp_file, delete_DS=True)
        return xr.open_zarr(cache_file, consolidated=True)
    elif file_format == 'netcdf':
        if (not os.path.exists(cache_file)) | clobber==True:
            obj.to_netcdf(cache_file, mode='w')
        return xr.open_dataset(cache_file, chunks={})
    else:
        raise ValueError("Unrecognised file_format. Options are 'zarr_DS', 'zarr_ZS' and 'netcdf'")
        
        
@xr.register_dataarray_accessor("xc")
class XCacheAccessor:
    def __init__(self, xarray_obj):
        self._obj = xarray_obj
        
    def cache(self, file_name=None, clobber=False, file_format='zarr_DS'):
        """
            Write an xarray object to disk and read it back

            Parameters
            ----------
            obj : xarray DataArray or Dataset
                data to cache and read back
            file_name : str, optional
                Name of file to write to disk. If not given, a random name will be generated
            clobber : boolean, optional
                If True, replace file if it already exists
            file_format : string, optional
                file format of the cached data. Options are 'zarr_DS' (zarr DirectoryStore), \
                'zarr_ZS' (zarr ZipStore), 'netcdf'

            Returns
            -------
            xarray DataArray or Dataset
                Same data as input, but now read directly from disk
        """

        name = self._obj.name if self._obj.name else '_cached_variable'
        return _cache(self._obj.to_dataset(name=name), file_name=file_name, clobber=clobber, file_format=file_format)[name]


@xr.register_dataset_accessor("xc")
class XCacheAccessor:
    def __init__(self, xarray_obj):
        self._obj = xarray_obj
        
    def cache(self, file_name=None, clobber=False, file_format='zarr_DS'):
        """
            Write an xarray object to disk and read it back

            Parameters
            ----------
            obj : xarray DataArray or Dataset
                data to cache and read back
            file_name : str, optional
                Name of file to write to disk. If not given, a random name will be generated
            clobber : boolean, optional
                If True, replace file if it already exists
            file_format : string, optional
                file format of the cached data. Options are 'zarr_DS' (zarr DirectoryStore), \
                'zarr_ZS' (zarr ZipStore), 'netcdf'

            Returns
            -------
            xarray DataArray or Dataset
                Same data as input, but now read directly from disk
        """

        return _cache(self._obj, file_name=file_name, clobber=clobber, file_format=file_format)