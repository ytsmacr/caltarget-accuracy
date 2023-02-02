import requests
import re
from tqdm import tqdm
import os
import pandas as pd

'''
by Cai Ytsma
last updated 9 November 2022

Automatically adds new LIBS CCS data from ChemCam PDS

Information: 
https://pds.nasa.gov/ds-view/pds/viewProfile.jsp?dsid=MSL-M-CHEMCAM-LIBS-4/5-RDR-V1.0
(MOC based on 'mean' CCS spectra)
'''

#---------------#
#  ADD SPECTRA  #
#---------------#
# prep
folder = input('Enter root folder path for data to be saved: ')
meta_path = f'{folder}\\LIBS_CCS_metadata.csv'
spectra_path = f'{folder}\\LIBS_CCS_mean_spectra.csv'
spectra = pd.read_csv(spectra_path)

# get page info
parent_url = 'https://pds-geosciences.wustl.edu/msl/msl-m-chemcam-libs-4_5-rdr-v1/mslccm_1xxx/data/'
# get page contents
page = requests.get(parent_url).text

# get sol pages
sol_pages = list(set(re.findall(r'sol\d{5}', page)))
sol_page_nos = [int(i[3:]) for i in sol_pages]
sol_page_nos.sort()

def no_to_sol(sol_no):
    n_zeros = 5 - len(str(sol_no))
    sol = 'sol'+(n_zeros*'0')+str(sol_no)
    return sol

# find which data needs adding
def get_sols_to_add():
    global meta_path
    meta = pd.read_csv(meta_path)
    most_recent_sol_no = max(meta['sol'])
    sol_to_add = [no_to_sol(i) for i in sol_page_nos if i > most_recent_sol_no]
    sol_to_add.sort()
    return sol_to_add, meta

sols_to_add, meta = get_sols_to_add()

def make_meta(meta_list):
    global meta, meta_path
    meta_to_add = pd.DataFrame(meta_list, columns = ['pkey','sol'])
    updated_meta = pd.concat([meta,meta_to_add], ignore_index=True).drop_duplicates(ignore_index=True)
    updated_meta.to_csv(meta_path, index=False)
    return updated_meta

def make_spectra(spectra_to_add):
    global spectra, spectra_path
    updated_spectra = spectra.merge(spectra_to_add)
    updated_spectra.to_csv(spectra_path, index=False)
    return updated_spectra

cont = True if len(sols_to_add) > 0 else False

while cont:
    try:
        meta_list = []
        count=0
        scount=0
        for sol in tqdm(sols_to_add, desc='new sols'):
            sol_page = requests.get(f'{parent_url}{sol}').text
            ccs_files = list(set(re.findall('.{13}ccs_.{19}\.csv', sol_page)))

            for filename in ccs_files:

                pkey = filename[:-4]

                s = pd.read_csv(f'{parent_url}{sol}/{filename}', 
                                skiprows=16)
                s.columns = [c.strip() for c in list(s.columns)]

                if count == 0:
                    spectra_to_add = s[['# wave','mean']].copy()
                    spectra_to_add.columns = ['wave', pkey]
                else:
                    spectra_to_add[pkey] = s['mean'].values

                meta_list.append([pkey, int(sol[3:])])
                count+=1 
            scount+=1 

        # export complete dfs
        meta = make_meta(meta_list)
        spectra = make_spectra(spectra_to_add)
        print(f'{count} spectra from {scount} sols added')
        cont=False

    except:
        # export what it got up to
        meta = make_meta(meta_list)
        spectra = make_spectra(spectra_to_add)
        print(f'{count} spectra from {scount} sols added')
        
        # prep for next iteration
        sols_to_add, meta = get_sols_to_add()
        if len(sols_to_add) == 0:
            cont = False
        
print('Spectra data up to date')

#---------------#
#    ADD MOC    #
#---------------#
moc_outpath = f'{folder}\\moc_composite.csv'

# get run MOC from current file
moc_df = pd.read_csv(moc_outpath)
moc_done = list(moc_df['Source File'].unique())
moc_done = set([m+'.csv' for m in moc_done])

# get those on the page
moc_path = parent_url+'moc/'
moc_page = requests.get(moc_path).text
moc_files = set(re.findall('moc.{10}\.csv', moc_page))

# add
moc_toadd = list(moc_files - moc_done)
moc_toadd.sort()

if len(moc_toadd) > 0:
    count = 0
    for moc in tqdm(moc_toadd):
        df = pd.read_csv(moc_path+moc, skiprows=6)

        # format
        # remove +/- columns
        df = df[[c for c in df.columns if '+/-' not in c]]
        # add pkey column
        df.insert(0,'pkey',[p[:-4].lower() for p in df.File])
        # don't need File anymore
        df.drop(columns='File', inplace=True)
        df['Source File'] = moc[:-4]

        if list(df.columns) != list(moc_df.columns):
            print(moc, 'has different format')
            display(moc_df.head(2))
            display(df.head(2))

        if count == 0:
            new_moc = pd.concat([moc_df, df], ignore_index=True)
        else:
            new_moc = pd.concat([new_moc, df], ignore_index=True)
        count+=1

    # export
    new_moc.to_csv(moc_outpath, index=False)
    
else:
    print('MOC data up to date')
    
#-----------------#
# ADD MOC TO META #
#-----------------#
meta_moc_path = f'{folder}\\LIBS_CCS_metadata_w_moc.csv'
meta_moc = meta.merge(new_moc)
meta_moc.to_csv(meta_moc_path, index=False)