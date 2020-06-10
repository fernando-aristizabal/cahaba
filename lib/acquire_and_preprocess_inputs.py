# -*- coding: utf-8 -*-
"""
Created on Thu Jun  4 10:58:34 2020

@author: bradford.bates
"""

import os
import csv
import sys
from multiprocessing import Pool

NUM_OF_WORKERS = 1

from utils.shared_variables import (NHD_URL_PARENT,
                                    NHD_URL_PREFIX,
                                    NHD_RASTER_URL_SUFFIX,
                                    NHD_VECTOR_URL_SUFFIX,
                                    NHD_RASTER_EXTRACTION_PREFIX,
                                    NHD_RASTER_EXTRACTION_SUFFIX,
                                    NHD_VECTOR_EXTRACTION_PREFIX,
                                    NHD_VECTOR_EXTRACTION_SUFFIX,
                                    PREP_PROJECTION,
                                    NWM_HYDROFABRIC_URL,
                                    HEADWATER_NODES)

from utils.shared_functions import pull_file
    

def project_and_convert_to_gpkg(args):
    input_gdb, gdb_layer, output_gpkg, projection = args[0], args[1], args[2], args[3]
    
    print("Projecting and converting " + gdb_layer)
    os.system('ogr2ogr -overwrite -progress -f GPKG -t_srs "{projection}" {output_gpkg} {input_gdb} {gdb_layer}'.format(projection=projection, output_gpkg=output_gpkg, input_gdb=input_gdb, gdb_layer=gdb_layer))


def pull_and_prepare_nwm_hydrofabric(path_to_saved_data_parent_dir):
    
    # -- Acquire and preprocess NWM data -- #
    nwm_hydrofabric_directory = os.path.join(path_to_saved_data_parent_dir, 'nwm_hydrofabric')
    if not os.path.exists(nwm_hydrofabric_directory):
        os.mkdir(nwm_hydrofabric_directory)
    pulled_hydrofabric_tar_gz_path = os.path.join(nwm_hydrofabric_directory, 'NWM_channel_hydrofabric.tar.gz')
    pull_file(NWM_HYDROFABRIC_URL, pulled_hydrofabric_tar_gz_path)
    
    os.system("7za x {pulled_hydrofabric_tar_gz_path} -o{nwm_hydrofabric_directory}".format(pulled_hydrofabric_tar_gz_path=pulled_hydrofabric_tar_gz_path, nwm_hydrofabric_directory=nwm_hydrofabric_directory))
    
    pulled_hydrofabric_tar_path = pulled_hydrofabric_tar_gz_path.strip('.gz')
    os.system("7za x {pulled_hydrofabric_tar_path} -o{nwm_hydrofabric_directory}".format(pulled_hydrofabric_tar_path=pulled_hydrofabric_tar_path, nwm_hydrofabric_directory=nwm_hydrofabric_directory))
    
    # Delete temporary zip files.
    os.remove(pulled_hydrofabric_tar_gz_path)
    os.remove(pulled_hydrofabric_tar_path)
    
    nwm_hydrofabric_gdb = os.path.join(nwm_hydrofabric_directory, 'NWM_v2.0_channel_hydrofabric', 'nwm_v2_0_hydrofabric.gdb')

    procs_list = []
    for nwm_layer in ['nwm_reaches_conus', 'nwm_waterbodies_conus', 'nwm_catchments_conus']:
        output_gpkg = os.path.join(nwm_hydrofabric_directory, nwm_layer + '.gpkg')
        procs_list.append([nwm_hydrofabric_gdb, nwm_layer, output_gpkg, PREP_PROJECTION])        
        
#    # Define paths to and append variables for any temporarily stored datasets for NWM hydrofabric.
#    headwater_nodes_gdb = os.path.dirname(HEADWATER_NODES)
#    print(headwater_nodes_gdb)
#    headwater_nodes_layer = os.path.split(HEADWATER_NODES)[1]
#    print(headwater_nodes_layer)
#    headwater_nodes_output_gpkg = os.path.join(nwm_hydrofabric_directory, 'nwm_headwaters.gpkg')
#    procs_list.append([headwater_nodes_gdb, headwater_nodes_layer, headwater_nodes_output_gpkg, PREP_PROJECTION])
#    
    pool = Pool(4)
    pool.map(project_and_convert_to_gpkg, procs_list)
        

def pull_and_prepare_nhd_data(procs_list):
    
    # Parse urls and extraction paths from procs_list.
    nhd_raster_download_url = procs_list[0]
    nhd_raster_extraction_path = procs_list[1]
    nhd_vector_download_url = procs_list[2]
    nhd_vector_extraction_path = procs_list[3]
    
    # Download raster and vector, if not already in user's directory (exist check performed by pull_file()).
    nhd_raster_extraction_parent = os.path.dirname(nhd_raster_extraction_path)
    if not os.path.exists(nhd_raster_extraction_parent):
        os.mkdir(nhd_raster_extraction_parent)
    pull_file(nhd_raster_download_url, nhd_raster_extraction_path)
    pull_file(nhd_vector_download_url, nhd_vector_extraction_path)
    
    # Unzip downloaded GDB.
    nhd_vector_extraction_parent = os.path.dirname(nhd_vector_extraction_path)
    huc = os.path.split(nhd_vector_extraction_parent)[1]  # Parse HUC.
    
    # Unzip vector and delete zipped file.
    os.system("7za x {nhd_vector_extraction_path} -o{nhd_vector_extraction_parent}".format(nhd_vector_extraction_path=nhd_vector_extraction_path, nhd_vector_extraction_parent=nhd_vector_extraction_parent))
    os.remove(nhd_vector_extraction_path)  # Delete the zipped GDB.
    
    # -- Project and convert NHDPlusBurnLineEvent and NHDPlusFlowLineVAA vectors to geopackage -- #
    nhd_gdb = nhd_vector_extraction_path.replace('.zip', '.gdb')  # Update extraction path from .zip to .gdb. 
    for nhd_layer in ['NHDPlusBurnLineEvent', 'NHDPlusFlowlineVAA']:
        output_gpkg = os.path.join(nhd_vector_extraction_parent, nhd_layer + huc + '.gpkg')
        project_and_convert_to_gpkg([nhd_gdb, nhd_layer, output_gpkg, PREP_PROJECTION])  # Use list because function is configured for multiprocessing.
    
    
def manage_preprocessing(hucs_of_interest_file_path, path_to_saved_data_parent_dir):
    """
    This functions manages the downloading and preprocessing of gridded and vector data for FIM production.
    
    Args:
        hucs_of_interest_file_path (str): Path to a user-supplied config file of hydrologic unit codes to be pulled and post-processed.
        path_to_saved_data_parent_dir (str): Path to directory where raw data and post-processed data will be saved.
    Returns: TBD
    """
    
    nhd_procs_list = []  # Initialize procs_list for multiprocessing.
    
    # Create the parent directory if nonexistent.
    if not os.path.exists(path_to_saved_data_parent_dir):
        os.mkdir(path_to_saved_data_parent_dir)
        
    # Create NHDPlus raster parent directory if nonexistent.
    nhd_raster_dir = os.path.join(path_to_saved_data_parent_dir, 'nhdplus_rasters')
    if not os.path.exists(nhd_raster_dir):
        os.mkdir(nhd_raster_dir)
        
    # Create the vector data parent directory if nonexistent.
    vector_data_dir = os.path.join(path_to_saved_data_parent_dir, 'nhdplus_vectors')
    if not os.path.exists(vector_data_dir):
        os.mkdir(vector_data_dir)
        
    # Parse HUCs from hucs_of_interest_file_path.
    with open(hucs_of_interest_file_path) as csv_file:  # Does not have to be CSV format.
        huc_list = list(csv.reader(csv_file))
        
    # Construct paths to data to download and append to procs_list for multiprocessed pull, project, and converstion to geopackage.
    for huc in huc_list:
        huc = str(huc[0])
    
        # Construct URL and extraction path for NHDPlus raster.
        nhd_raster_download_url = os.path.join(NHD_URL_PARENT, NHD_URL_PREFIX + huc + NHD_RASTER_URL_SUFFIX)
        nhd_raster_extraction_path = os.path.join(nhd_raster_dir, NHD_RASTER_EXTRACTION_PREFIX + huc, NHD_RASTER_EXTRACTION_SUFFIX)
        
        # Construct URL and extraction path for NHDPlus vector. Organize into huc-level subdirectories.
        nhd_vector_download_url = os.path.join(NHD_URL_PARENT, NHD_URL_PREFIX + huc + NHD_VECTOR_URL_SUFFIX)
        nhd_vector_download_parent = os.path.join(vector_data_dir, huc)
        if not os.path.exists(nhd_vector_download_parent):
            os.mkdir(nhd_vector_download_parent)
        nhd_vector_extraction_path = os.path.join(nhd_vector_download_parent, NHD_VECTOR_EXTRACTION_PREFIX + huc + NHD_VECTOR_EXTRACTION_SUFFIX)

        # Append extraction instructions to nhd_procs_list.
        nhd_procs_list.append([nhd_raster_download_url, nhd_raster_extraction_path, nhd_vector_download_url, nhd_vector_extraction_path])
        
    # Pull NHD Data.
    pool = Pool(NUM_OF_WORKERS)
    pool.map(pull_and_prepare_nhd_data, nhd_procs_list)
    
    pull_and_prepare_nwm_hydrofabric(path_to_saved_data_parent_dir)


if __name__ == '__main__':
    
    # Get input arguments from command line.
    hucs_of_interest_file_path = sys.argv[1]
    path_to_saved_data_parent_dir = sys.argv[2]  # The parent directory for all saved data.

    
    manage_preprocessing(hucs_of_interest_file_path, path_to_saved_data_parent_dir)

    
    
    
    