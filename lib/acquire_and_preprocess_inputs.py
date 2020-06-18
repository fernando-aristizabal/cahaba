# -*- coding: utf-8 -*-
"""
Created on Thu Jun  4 10:58:34 2020

@author: bradford.bates
"""

import os
import csv
import sys
from multiprocessing import Pool
import geopandas as gp

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
                                    WBD_NATIONAL_URL,
                                    FOSS_ID)

from utils.shared_functions import pull_file, run_system_command, subset_wbd_gpkg
    

NHDPLUS_VECTORS_DIRNAME = 'nhdplus_vectors'
NHDPLUS_RASTERS_DIRNAME = 'nhdplus_rasters'

def pull_and_prepare_wbd(path_to_saved_data_parent_dir):
    """
    This helper function pulls and unzips Watershed Boundary Dataset (WBD) data. It uses the WBD URL defined by WBD_NATIONAL_URL.
    
    Args:
        path_to_saved_data_parent_dir (str): The system path to where the WBD will be downloaded, unzipped, and preprocessed.
    
    """
    
    # -- Acquire and preprocesses NWM data -- #
    wbd_directory = os.path.join(path_to_saved_data_parent_dir, 'wbd')
    if not os.path.exists(wbd_directory):
        os.mkdir(wbd_directory)
    
    wbd_gdb_path = os.path.join(wbd_directory, 'WBD_National_GDB.gdb')
    
    # Pull and unzip WBD_National_GDB.zip and project and convert to geopackage if not already done previously.
    if not os.path.exists(wbd_gdb_path):
        pulled_wbd_zipped_path = os.path.join(wbd_directory, 'WBD_National_GDB.zip')
        pull_file(WBD_NATIONAL_URL, pulled_wbd_zipped_path)
        os.system("7za x {pulled_wbd_zipped_path} -o{wbd_directory}".format(pulled_wbd_zipped_path=pulled_wbd_zipped_path, wbd_directory=wbd_directory))
        
        procs_list, wbd_gpkg_list = [], []
        # Add fossid to HU8, project, and convert to geopackage. Code block from Brian Avant.
        print("Adding fossid to HU8...")
        wbd_hu8 = gp.read_file(wbd_gdb_path, layer='WBDHU8')
        wbd_hu8 = wbd_hu8.sort_values('HUC8')
        n_recs = len(wbd_hu8)
        fossids = [str(item).zfill(4) for item in list(range(1, 1 + n_recs))]
        wbd_hu8[FOSS_ID] = fossids
        wbd_hu8 = wbd_hu8.to_crs(PREP_PROJECTION)
        wbd_hu8.to_file(os.path.join(wbd_directory, 'WBDHU8.gpkg'), driver='GPKG')
        wbd_gpkg_list.append([os.path.join(wbd_directory, 'WBDHU8.gpkg'), os.path.join(wbd_directory, 'WBDHU8_conus.gpkg')])
        
        del wbd_hu8, fossids, n_recs
        
        for wbd_layer in ['WBDHU4', 'WBDHU6']:
            output_gpkg = os.path.join(wbd_directory, wbd_layer + '.gpkg')
            output_gpkg_conus = os.path.join(wbd_directory, wbd_layer + '_conus.gpkg')
            wbd_gpkg_list.append([output_gpkg, output_gpkg_conus])
            procs_list.append(['ogr2ogr -overwrite -progress -f GPKG -t_srs "{projection}" {output_gpkg} {wbd_gdb_path} {wbd_layer}'.format(output_gpkg=output_gpkg, wbd_gdb_path=wbd_gdb_path, wbd_layer=wbd_layer, projection=PREP_PROJECTION)])
       
    pool = Pool(3)
    pool.map(run_system_command, procs_list)
    
    # Subset WBD layers to CONUS.
    print("Subsetting WBD layers to CONUS...")
    for pair in wbd_gpkg_list:
        subset_wbd_gpkg(pair)
        
    return(wbd_directory)
            

def pull_and_prepare_nwm_hydrofabric(path_to_saved_data_parent_dir):
    """
    This helper function pulls and unzips NWM hydrofabric data. It uses the NWM hydrofabric URL defined by NWM_HYDROFABRIC_URL.
    
    Args:
        path_to_saved_data_parent_dir (str): The system path to where the NWM hydrofabric will be downloaded, unzipped, and preprocessed.
        
    """
    
    
    # -- Acquire and preprocess NWM data -- #
    nwm_hydrofabric_directory = os.path.join(path_to_saved_data_parent_dir, 'nwm_hydrofabric')
    if not os.path.exists(nwm_hydrofabric_directory):
        os.mkdir(nwm_hydrofabric_directory)
    pulled_hydrofabric_tar_gz_path = os.path.join(nwm_hydrofabric_directory, 'NWM_channel_hydrofabric.tar.gz')
    
    nwm_hydrofabric_gdb = os.path.join(nwm_hydrofabric_directory, 'NWM_v2.0_channel_hydrofabric', 'nwm_v2_0_hydrofabric.gdb')

    if not os.path.exists(nwm_hydrofabric_gdb):  # Only pull and unzip if the files don't already exist.
        pull_file(NWM_HYDROFABRIC_URL, pulled_hydrofabric_tar_gz_path)
        os.system("7za x {pulled_hydrofabric_tar_gz_path} -o{nwm_hydrofabric_directory}".format(pulled_hydrofabric_tar_gz_path=pulled_hydrofabric_tar_gz_path, nwm_hydrofabric_directory=nwm_hydrofabric_directory))
        
        pulled_hydrofabric_tar_path = pulled_hydrofabric_tar_gz_path.strip('.gz')
        os.system("7za x {pulled_hydrofabric_tar_path} -o{nwm_hydrofabric_directory}".format(pulled_hydrofabric_tar_path=pulled_hydrofabric_tar_path, nwm_hydrofabric_directory=nwm_hydrofabric_directory))
        
        # Delete temporary zip files.
        os.remove(pulled_hydrofabric_tar_gz_path)
        os.remove(pulled_hydrofabric_tar_path)
        
        # Project and convert to geopackage.
        print("Projecting and converting NWM layers to geopackage...")
        procs_list = []
        for nwm_layer in ['nwm_reaches_conus', 'nwm_waterbodies_conus', 'nwm_catchments_conus']:
            output_gpkg = os.path.join(nwm_hydrofabric_directory, nwm_layer + '.gpkg')
            procs_list.append(['ogr2ogr -overwrite -progress -f GPKG -t_srs "{projection}" {output_gpkg} {nwm_hydrofabric_gdb} {nwm_layer}'.format(projection=PREP_PROJECTION, output_gpkg=output_gpkg, nwm_hydrofabric_gdb=nwm_hydrofabric_gdb, nwm_layer=nwm_layer)])        
            
        pool = Pool(4)
        pool.map(run_system_command, procs_list)
            

def pull_and_prepare_nhd_data(args):
    """
    This helper function is designed to be multiprocessed. It pulls and unzips NHD raster and vector data.
    
    Args:
        args (list): A list of arguments in this format: [nhd_raster_download_url, nhd_raster_extraction_path, nhd_vector_download_url, nhd_vector_extraction_path]
        
    """
    
    # Parse urls and extraction paths from procs_list.
    nhd_raster_download_url = args[0]
    nhd_raster_extraction_path = args[1]
    nhd_vector_download_url = args[2]
    nhd_vector_extraction_path = args[3]
    
    nhd_gdb = nhd_vector_extraction_path.replace('.zip', '.gdb')  # Update extraction path from .zip to .gdb. 

    # Download raster and vector, if not already in user's directory (exist check performed by pull_file()).
    nhd_raster_extraction_parent = os.path.dirname(nhd_raster_extraction_path)
    if not os.path.exists(nhd_raster_extraction_parent):
        os.mkdir(nhd_raster_extraction_parent)
        
    if not os.path.exists(nhd_gdb):  # Only pull if not already pulled and processed.
        pull_file(nhd_raster_download_url, nhd_raster_extraction_path)
        pull_file(nhd_vector_download_url, nhd_vector_extraction_path)
        
        # Unzip downloaded GDB.
        nhd_vector_extraction_parent = os.path.dirname(nhd_vector_extraction_path)
        huc = os.path.split(nhd_vector_extraction_parent)[1]  # Parse HUC.
        
        # Unzip vector and delete zipped file.
        os.system("7za x {nhd_vector_extraction_path} -o{nhd_vector_extraction_parent}".format(nhd_vector_extraction_path=nhd_vector_extraction_path, nhd_vector_extraction_parent=nhd_vector_extraction_parent))
        os.remove(nhd_vector_extraction_path)  # Delete the zipped GDB.
        
        # -- Project and convert NHDPlusBurnLineEvent and NHDPlusFlowLineVAA vectors to geopackage -- #
        for nhd_layer in ['NHDPlusBurnLineEvent', 'NHDPlusFlowlineVAA']:
            output_gpkg = os.path.join(nhd_vector_extraction_parent, nhd_layer + huc + '.gpkg')
            run_system_command(['ogr2ogr -overwrite -progress -f GPKG -t_srs "{projection}" {output_gpkg} {nhd_gdb} {nhd_layer}'.format(projection=PREP_PROJECTION, output_gpkg=output_gpkg, nhd_gdb=nhd_gdb, nhd_layer=nhd_layer)])  # Use list because function is configured for multiprocessing.
    
    
def build_huc_list_files(path_to_saved_data_parent_dir, wbd_directory):
    
    # Identify all saved NHDPlus Vectors.
    nhd_plus_vector_dir = os.path.join(path_to_saved_data_parent_dir, NHDPLUS_VECTORS_DIRNAME)
    
    huc4_list = os.listdir(nhd_plus_vector_dir)
        
    huc6_list, huc8_list = [], []
    # Read WBD into dataframe.
    
    huc_gpkg_list = ['WBDHU6', 'WBDHU8']
    
    for huc_gpkg in huc_gpkg_list:
        full_huc_gpkg = os.path.join(wbd_directory, huc_gpkg + '_conus.gpkg')
    
        # Open geopackage.
        wbd = gp.read_file(full_huc_gpkg)
        gdf = gp.GeoDataFrame(wbd)
        
        # Loop through entries and compare against the huc4_list to get available HUCs within the geopackage domain.
        for index, row in gdf.iterrows():
            huc = row["HUC" + huc_gpkg[-1]]
            
            # Append huc to appropriate list.
            if str(huc[:4]) in huc4_list:
                if huc_gpkg == 'WBDHU6':
                    huc6_list.append(huc)
                elif huc_gpkg == 'WBDHU8':
                    huc8_list.append(huc)       
    
    # Write huc lists to appropriate .lst files.
    included_huc4_file = os.path.join(path_to_saved_data_parent_dir, 'included_huc4.lst')
    included_huc6_file = os.path.join(path_to_saved_data_parent_dir, 'included_huc6.lst')
    included_huc8_file = os.path.join(path_to_saved_data_parent_dir, 'included_huc8.lst')
    
    with open(included_huc4_file, 'w') as f:
        for item in huc4_list:
            f.write("%s\n" % item)
            
    with open(included_huc6_file, 'w') as f:
        for item in huc6_list:
            f.write("%s\n" % item)
            
    with open(included_huc8_file, 'w') as f:
        for item in huc8_list:
            f.write("%s\n" % item)
    
    
def manage_preprocessing(hucs_of_interest_file_path, path_to_saved_data_parent_dir):
    """
    This functions manages the downloading and preprocessing of gridded and vector data for FIM production.
    
    Args:
        hucs_of_interest_file_path (str): Path to a user-supplied config file of hydrologic unit codes to be pulled and post-processed.
        path_to_saved_data_parent_dir (str): Path to directory where raw data and post-processed data will be saved.
        
    """
    
    nhd_procs_list = []  # Initialize procs_list for multiprocessing.
    
    # Create the parent directory if nonexistent.
    if not os.path.exists(path_to_saved_data_parent_dir):
        os.mkdir(path_to_saved_data_parent_dir)
        
    # Create NHDPlus raster parent directory if nonexistent.
    nhd_raster_dir = os.path.join(path_to_saved_data_parent_dir, NHDPLUS_RASTERS_DIRNAME)
    if not os.path.exists(nhd_raster_dir):
        os.mkdir(nhd_raster_dir)
        
    # Create the vector data parent directory if nonexistent.
    vector_data_dir = os.path.join(path_to_saved_data_parent_dir, NHDPLUS_VECTORS_DIRNAME)
    if not os.path.exists(vector_data_dir):
        os.mkdir(vector_data_dir)
        
    # Parse HUCs from hucs_of_interest_file_path.
    with open(hucs_of_interest_file_path) as csv_file:  # Does not have to be CSV format.
        huc_list = list(csv.reader(csv_file))
        
    # Construct paths to data to download and append to procs_list for multiprocessed pull, project, and converstion to geopackage.
    for huc in huc_list:
        huc = str(huc[0])  # Ensure huc is string.
    
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
        
    # Pull and prepare NHD data.
    pool = Pool(3)
    pool.map(pull_and_prepare_nhd_data, nhd_procs_list)
    
    # Pull and prepare NWM data.
#    pull_and_prepare_nwm_hydrofabric(path_to_saved_data_parent_dir)  # Commented out for now.

    # Pull and prepare WBD data.
    wbd_directory = pull_and_prepare_wbd(path_to_saved_data_parent_dir)
    
    # Create HUC list files.
    build_huc_list_files(path_to_saved_data_parent_dir, wbd_directory)
    

if __name__ == '__main__':
    
    # Get input arguments from command line.
    hucs_of_interest_file_path = sys.argv[1]
    path_to_saved_data_parent_dir = sys.argv[2]  # The parent directory for all saved data.
    
    manage_preprocessing(hucs_of_interest_file_path, path_to_saved_data_parent_dir)
            