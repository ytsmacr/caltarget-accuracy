# Files to download LIBS data from MSL and Mars 2020 missions from NASA's PDS
## by Cai Ytsma (cai@caiconsulting.co.uk)

### ** New in this version ** 
#### Programs bulk extract all records and export dated files rather than appending newly released data, due to potential adjustments to previous files.
### *************************

Install Python packages from `requirements.txt` before running programs. 

If the connection breaks during the procedure, it will automatically store and update what data it has pulled so far and then continue.

Feel free to edit as needed!

#### For both programs:

Mean spectra data (the basis for MOC calculations) are collated into a single .csv with individual filename headers.

Metadata (sol, filename, sample name, etc.) are extracted and merged with MOC data. 

Lastly, spectra and metadata files are merged into a single file compatible with the Python Hyperspectral Analysis Tool (PyHAT): https://doi.org/10.1016/B978-0-12-818721-0.00012-4 

#### SuperCam
Clean, calibrated spectral data are extracted from https://pds-geosciences.wustl.edu/m2020/urn-nasa-pds-mars2020_supercam/data_calibrated_spectra/

Predicted major element abundances (MOC) extracted from `supercam_libs_moc.csv` here: https://pds-geosciences.wustl.edu/m2020/urn-nasa-pds-mars2020_supercam/data_derived_spectra/

Program outputs three folders for 1) individual .fits files, as well as 2) laser data and 3) spectra extracted from the .fits files. `These outputs can be adjusted for individual need - data information is documented here: https://pds-geosciences.wustl.edu/m2020/urn-nasa-pds-mars2020_supercam/document/M2020_SuperCam_EDR_RDR_SIS.pdf`

#### ChemCam
Clean, calibrated spectral data are extracted from https://pds-geosciences.wustl.edu/m2020/urn-nasa-pds-mars2020_supercam/data_derived_spectra/

Predicted major element abundances (MOC) extracted from https://pds-geosciences.wustl.edu/msl/msl-m-chemcam-libs-4_5-rdr-v1/mslccm_1xxx/data/moc/
