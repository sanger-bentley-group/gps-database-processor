# GPS Database Processor

GPS Database Processor is an all-in-one tool for processing GPS ([Global Pneumococcal Sequencing Project](https://www.pneumogen.net/gps/)) database updates. 

This tool takes the GPS database's three `.csv` (comma-separated values) source files as input:
- `table1` - metadata
- `table2` - QC
- `table3` - analysis

The tool carries out several operations in the following order:
1. Validation of columns and values in the `.csv` input files
   - The terminal output displays any unexpected or erroneous values
   - For columns that should only contain UPPERCASE strings, any lowercase value will be converted to UPPERCASE; if a case conversion occurs in a table, a copy of the original file will be saved with `_original` at the end, and then the updated table will be saved in-place
   - If there are any critical errors, the tool will terminate its process and will not carry out the subsequent operations
2. Generate `table4` using inferred data based on `table1`, `table3` and reference tables in the `data` directory
   - If there is a location that does not exist in `data/coordinates.csv` (one of the reference tables), it will attempt the fetch the information via Mapbox API
       - The first time this is triggered, it will ask for your Mapbox API key (access token) and save it locally at `config/api_keys.confg` for future use
       - For more information on the Mapbox API key (access token), please visit [their documentation](https://docs.mapbox.com/help/glossary/access-token/)
3. Generate [Monocle](https://data.monocle.sanger.ac.uk/)-ready `.csv` files in 3-table format
4. Generate `data.json` for [GPS Database Overview](https://www.pneumogen.net/gps/gps-database-overview/)

&nbsp;
## Workflow
![Workflow Diagram](doc/workflow.drawio.svg)

&nbsp;
## Usage
### Requirement
- Conda / Mamba
- Git

### Setup
1. Clone this repository to your machine
   ```
   git clone https://github.com/HarryHung/gps-database-processor.git
   ```
2. Go into the local copy of the repository 
   ```
   cd gps-database-processor
   ```
3. Setup Conda Environment (If using Mamba, replace `conda` with `mamba` in the following commands) 
   ```
   conda env create -f environment.yml
   ```

### Run
1. Pull any updates from remote repository
   ```
   git pull
   ```
2. Activate the Conda Environment
   ```
   conda activate gps-db-processor
   ```
3. Put the GPS database's three `.csv` source files into the directory containing the cloned repository
4. Run the following command to validate your input files and generate the output files:
   ```
   python3 processor.py
   ```
5. If you or the tool have updated any of the reference tables (i.e. any file in the `data` directory), create a PR (Pull Request) on this repository

### Optional arguments
- By default, this tool assumes the file names of `table1`, `table2` and `table3` to be `table1.csv`, `table2.csv`, and `table3.csv` respectively, and the output file name of `table4` will be `table4.csv`, these can be changed by using optional arguments; The 3 Monocle-ready `.csv` files will use the file names of `table1`, `table2` and `table3` with `_monocle` appended at the end; The data file for GPS Database Overview will always have the file name `data.json`
- Available optional arguments:
  - `-m your_file_name.csv` or `--meta your_file_name.csv`: Override the default `table1` (metadata) file name of `table1.csv`
  - `-q your_file_name.csv` or `--qc your_file_name.csv`: Override the default `table2` (qc) file name of `table2.csv`
  - `-a your_file_name.csv` or `--analysis your_file_name.csv`: Override the default `table3` (analysis) file name of `table3.csv`
  - `-o your_file_name.csv` or `--output your_file_name.csv`: Override the default `table4` (inferred data) file name of `table4.csv`
- Example command using optional arguments:
  ```
  python processor.py -m table1_meta.csv -q table2_qc.csv -a table3_analysis.csv -o table4_inferred.csv
  ```

### Reference Tables (files in the `data` directory)
- `alpha2_country.csv` 
  - Map `Country` in `table1` to [ISO 3166-1 alpha-2 code](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2) for `data.json` generation
  - 2 columns: `alpha2`, `country` - representing ISO 3166-1 alpha-2 code, country name respectively
  - Modify the value in the `country` column if it does not match the value used in `Country` in `table1`
- `coordinates.csv`
  - Map the combination of `Country`, `Region`, `City` in `table1` to `Latitude`, `Longitude` for `table4` generation
  - 3 columns: `Country-Region-City`, `Latitude`, `Longitude` - representing Country-Region-City combination, latitude, longitude respectively
  - If a new `Country-Region-City` combination is found, this tool will attempt to auto-assign `Latitude`, `Longitude` to it using `geopy` and add them to this file
  - Modify values in the `Latitude`, `Longitude` columns if incorrect values are assigned to them by `geopy`
- `manifestations.csv`
  - Map the combination of `Clinical_manifestation` and `Source` in `table1` to `Manifestation` for `table4` generation
  - 3 columns: `Clinical_manifestation`, `Source`, `Manifestation` - representing clinical manifestation, sample source, resulting manifestation respectively
  - Add new `Clinical_manifestation` and `Source` combinations and their resulting `Manifestation` to this file
- `non_standard_ages.csv`
  - Map non-numeric values in `Age_years` in `table1` to `less_than_5years_old` for `table4` generation
  - 2 columns: `value`, `less_than_5years_old` - representing non-numeric age value, whether the value is less than 5 years old respectively
  - Add new non-numeric `Age_years` values and whether they are `less_than_5years_old` (Y or N) to this file
- `pcv_introduction_year.csv`
  - Map `Country`, `Year` in `table1` to `Vaccine_period`, `Introduction_year`, `PCV_type` for `table4` generation
  - 3 columns: `Country`, `PCV_type`, `Introduction_year` - representing country name, PCV type, introduction year of the PCV respectively 
  - Add new `Country`, `PCV_type`, `Introduction_year` to this file when that country introduces PCV or has a change of PCV type in their National Immunisation/Vaccination Programme
- `pcv_valency.csv`
  - Map `In_silico_serotype` in `table3` to all PCV types under `PCV_type` of this file for `table4` generation
  - 2 columns: `PCV_type`, `Serotypes` - representing PCV type, list of covered serotypes by that PCV type respectively
  - Add new `PCV_type`, `Serotypes` to this file when there is a new PCV type
- `published_public_names.txt`
  - Map `Public_name` in `table1` and `table3` to `Published` for `table4` generation
  - A list of `Public_name` of all samples that have been published


&nbsp;
## Requirements & Compatibility
GPS Database requirement:
- GPS1 v4.0+

Tested on:
- [Python](https://www.python.org/) 3.11
- [pandas](https://pandas.pydata.org/) 1.5.2
- [geopy](https://github.com/geopy/geopy) 2.3.0


&nbsp;
## Credits
This project uses Open Source components. You can find the source code of their open source projects along with license information below. I acknowledge and am grateful to these developers for their contributions to open source.

[**pandas**](https://pandas.pydata.org/)
- Copyright (c) 2008-2011, AQR Capital Management, LLC, Lambda Foundry, Inc. and PyData Development Team. All rights reserved.
- Copyright (c) 2011-2022, Open source contributors.
- License (BSD-3-Clause): https://github.com/pandas-dev/pandas/blob/main/LICENSE

[**geopy**](https://github.com/geopy/geopy)
- © geopy contributors 2006-2018 under the MIT License.
- License (MIT): https://github.com/geopy/geopy/blob/master/LICENSE

[**draw.io / diagrams.net**](https://www.diagrams.net/)
- draw.io is owned and developed by JGraph Ltd, a UK based software company.
- License (Apache License 2.0): https://github.com/jgraph/drawio/blob/dev/LICENSE