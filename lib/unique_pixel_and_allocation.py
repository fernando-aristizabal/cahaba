# -*- coding: utf-8 -*-
"""
Created on Fri Oct 16 07:51:56 2020

@author: trevor.grout
"""
import rasterio
import numpy as np
import argparse
from r_grow_distance import r_grow_distance

def stream_pixel_zones(stream_pixels, unique_stream_pixels, grass_workspace):
    '''
    This function will assign a unique ID for each stream pixel and writes to file. It then uses this raster to run GRASS r.grow.distance tool to create the allocation and proximity rasters required to complete the lateral thalweg conditioning. 
    
    Parameters
    ----------
    stream_pixels : STR
        Path to stream raster with value of 1. For example, demDerived_streamPixels.tif.
    unique_stream_pixels : STR
        Output path of raster containing unique ids for each stream pixel.
    grass_workspace : STR
        Path to temporary GRASS directory which is deleted.

    Returns
    -------
    distance_grid : STR
        Path to output proximity raster (in meters).
    allocation_grid : STR
        Path to output allocation raster.

    '''

    # Import stream pixel raster
    with rasterio.open(stream_pixels) as temp:
        streams_profile = temp.profile
        streams = temp.read(1)  

    # Create array that matches shape of streams raster with unique values for each cell.
    unique_vals = (np.arange(streams.size)).reshape(*streams.shape)

    # At streams return the unique array value otherwise return -1 (this will be the nodata value)
    stream_pixel_values = np.where(streams == 1, unique_vals, -1)
    
    # Reassign nodata to be -1 and dtype to be consistent with stream_pixel_values
    streams_profile.update(nodata = -1)
    streams_profile.update(dtype = stream_pixel_values.dtype)
    
    # Output to raster 
    with rasterio.Env():
        with rasterio.open(unique_stream_pixels, 'w', **streams_profile) as raster:
            raster.write(stream_pixel_values,1)
    
    # Compute allocation and proximity grid using r.grow.distance. Output distance grid in meters.
    distance_grid, allocation_grid = r_grow_distance(unique_stream_pixels, grass_workspace)

    return distance_grid, allocation_grid

if __name__ == '__main__':

    #Parse arguments
    parser = argparse.ArgumentParser(description = 'Produce unique stream pixel values and allocation/proximity grids')
    parser.add_argument('-s', '--stream', help = 'raster to perform r.grow.distance', required = True)
    parser.add_argument('-o', '--out', help = 'output raster of unique ids for each stream pixel', required = True)
    parser.add_argument('-g', '--grass_workspace', help = 'Temporary GRASS workspace', required = True)

    # Extract to dictionary and assign to variables.
    args = vars(parser.parse_args())

    # Rename variable inputs
    stream_pixels = args['stream']
    unique_stream_pixels = args['out']
    grass_workspace = args['grass_workspace']
    
    # Run stream_pixel_zones
    stream_pixel_zones(stream_pixels, unique_stream_pixels, grass_workspace)



