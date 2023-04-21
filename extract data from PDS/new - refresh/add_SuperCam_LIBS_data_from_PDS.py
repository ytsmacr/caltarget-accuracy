from astropy.io import fits
from astropy.table import Table
#https://hst-docs.stsci.edu/hstdhb/4-hst-data-analysis/4-4-working-with-fits-data-in-python

import requests
import re
from tqdm import tqdm
import os
import pandas as pd
import numpy as np
import datetime

'''
by Cai Ytsma (cai@caiconsulting.co.uk)
last updated 21 April 2023

Automatically extracts LIBS RDR data from SuperCam PDS
and converts to PyHAT format

Data documentation:
https://pds-geosciences.wustl.edu/m2020/urn-nasa-pds-mars2020_supercam/document/SuperCam_Bundle_SIS.pdf
'''

# prep 
date = datetime.datetime.now().strftime("%d%m%y")

parent_url = 'https://pds-geosciences.wustl.edu/m2020/urn-nasa-pds-mars2020_supercam/data_calibrated_spectra/'

folder = input('Enter root folder path for data to be saved: ')
laser_folder = f'{folder}\\LIBS RDR laser data'
spectra_folder = f'{folder}\\LIBS RDR spectra'
fits_folder = f'{folder}\\LIBS RDR fits files'

meta_path = f'{folder}\\LIBS_RDR_metadata_{date}.csv'
meta_comps_path = f'{folder}\\LIBS_RDR_metadata_w_pred_comps_{date}.csv'
spectra_path = f'{folder}\\LIBS_RDR_mean_spectra_{date}.csv'

****spectra = pd.read_csv(spectra_path)

# first, update comps file
comps = pd.read_csv('https://pds-geosciences.wustl.edu/m2020/urn-nasa-pds-mars2020_supercam/data_derived_spectra/supercam_libs_moc.csv')
comps_path =  f'{folder}\\supercam_libs_moc_{date}.csv'
comps.to_csv(comps_path, index=False)

# drop header
comps = comps.iloc[7:].copy()
comps.columns = list(comps.iloc[0])
comps.drop(index=7, inplace=True)
comps.reset_index(inplace=True, drop=True)
comps.rename(columns={'cdr_fname':'pkey'}, inplace=True)
# add version 01 to make match meta values
comps['pkey'] = [x+'01' for x in comps.pkey]

# get page contents
page = requests.get(parent_url).text

# get sol pages
def get_sol_no(sol_page):
    return int(sol_page.split('_')[1])

def no_to_sol(sol_no):
    n_zeros = 5 - len(str(sol_no))
    sol = 'sol_'+(n_zeros*'0')+str(sol_no)
    return sol

sol_pages = list(set(re.findall(r'sol_\d{5}', page)))
sol_pages.sort()
sol_page_nos = [get_sol_no(i) for i in sol_pages]

# find which data needs adding
def get_sols_to_add():
    global meta_path, sol_page_nos
    
    # if already exists because the run broke
    if os.path.exists(meta_path):
        meta = pd.read_csv(meta_path)
        most_recent_sol_no = max(meta['sol'])
        most_recent_sol = no_to_sol(most_recent_sol_no)
        sol_to_add = [i for i in sol_pages if i > most_recent_sol]
        sol_to_add.sort()
        
    # to initiate
    else:
        meta = pd.DataFrame(columns=['pkey',
                                     'sol',
                                     'sclock',
                                     'seq_n',
                                     'target',
                                     'location_n',
                                     'producer',
                                     'version'])
        sol_to_add = sol_pages
        
    return sol_to_add, meta

sols_to_add, meta = get_sols_to_add()

def make_meta(meta_dict):
    global meta, meta_path
    new_meta = pd.DataFrame.from_dict(meta_dict, orient='index').reset_index()
    new_meta.columns = list(meta.columns)
    updated_meta = pd.concat([meta,new_meta], ignore_index=True).drop_duplicates(ignore_index=True)
    updated_meta.to_csv(meta_path, index=False)
    return updated_meta

def make_spectra(spectra_to_add):
    global spectra_path
    
    # if already exists because the run broke
    if os.path.exists(spectra_path):
        global spectra
        updated_spectra = spectra.merge(spectra_to_add, on='wave')
    
    # to initiate
    else:
        updated_spectra = spectra_to_add.copy()
    
    updated_spectra.to_csv(spectra_path, index=False)
    return updated_spectra

cont = True if len(sols_to_add) > 0 else False
    
while cont:
    try:
        meta_dict = dict()
        spectra_dict = dict()
        count=0
        scount=0
        for sol in tqdm(sols_to_add, desc='new sols'):

            sol_page = requests.get(parent_url+sol).text
            fits_files = list(set(re.findall('.{65}\.fits', sol_page)))

            for filename in fits_files:

                # get product type
                prod_type = filename.split('_')[4]

                # continue if LIBS RDR type
                if re.match('^cl.$', prod_type):
                    
                    pkey = filename[:-5]

                    # download file
                    data = requests.get(f'{parent_url}{sol}/{filename}').content
                    with open(f'{fits_folder}\\{filename}', 'wb') as file:
                        file.write(data)
                        file.close()

                    data = fits.open(f'{fits_folder}\\{filename}')

                    # laser data
                    laser = pd.DataFrame(data[5].data)
                    for col in laser.columns:
                        laser[col] = laser[col].astype('float64')
                    laser.to_csv(f'{laser_folder}\\{filename[:-5]}.csv', index=False)

                    # spectral data
                    s = pd.DataFrame(data[6].data)  
                    for col in s.columns:
                        s[col] = s[col].astype('float64')

                    stats = pd.DataFrame(data[7].data)  
                    for col in stats.columns:
                        stats[col] = stats[col].astype('float64')

                    wave = pd.DataFrame(data[8].data)
                    for col in wave.columns:
                        wave[col] = wave[col].astype('float64')

                    saturation = pd.DataFrame(data[9].data)

                    # merge
                    big_df = wave.join(stats).join(s).join(saturation)
                    big_df.to_csv(f'{spectra_folder}\\{pkey}.csv', index=False)
                    
                    # get mean spectrum to add to spectra file
                    if count==0:
                        spectra_to_add = big_df[['Wavelength','Mean']].copy()
                        spectra_to_add.columns = ['wave',pkey]
                    else:
                        spectra_to_add[pkey] = list(big_df['Mean'].values)

                    # add metadata
                    info = pkey.split('_')
                    meta_dict[pkey] = {   
                        'sol':info[1],
                        'sclock':'_'.join(info[2:4]),
                        'seq_n':info[5],
                        'target':filename[39:60].replace('_',' ').strip(),
                        'point_n':filename[60:62],
                        'producer':filename[62],
                        'version':filename[63:65]
                    }

                    count+=1

                # wrong file type
                else:
                    continue
                    
            scount+=1

        # export complete dfs
        meta = make_meta(meta_dict)
        spectra = make_spectra(spectra_to_add)
        print(f'{count} spectra from {scount} sols added')
        cont=False

    except:
        meta = make_meta(meta_dict)
        spectra = make_spectra(spectra_to_add)
        print(f'{count} spectra from {scount} sols added')
        
        # prep for next iteration
        sols_to_add, meta = get_sols_to_add()
        if len(sols_to_add) == 0:
            cont = False

print('Spectra data extracted')

# finally, add predicted compositions
meta_w_comps = meta.merge(comps)
meta_w_comps.to_csv(meta_comps_path, index=False)

#-------------------------#
# CONVERT TO PYHAT FORMAT #
#-------------------------#
print('Converting to PyHAT format...')

# prepare spectra
spectra = spectra.T.reset_index()
spectra.columns = list(spectra.iloc[0])
spectra.drop(index=0, inplace=True)
spectra.rename(columns={'wave':'pkey'}, inplace=True)
if np.nan in spectra.columns:
    spectra.drop(columns=np.nan, inplace=True)
    
# merge with metadata
big_df = meta_w_comps.merge(spectra)

# add column labels needed for PyHAT
col_list = list(big_df.columns)
new_cols = ['meta']*8 + ['comp']*19 + ['wvl']*7462
col_df = pd.DataFrame([new_cols, col_list], columns=col_list)
big_df = pd.concat([col_df, big_df])

# export
big_df.to_csv(f'{folder}\\LIBS_RDR_data_PyHAT_{date}.csv', index=False, header=False)
print('Finished')