# GPS Database Processor - Work-In-Progress

GPS Database Processor is an all-in-one tool for processing GPS ([Global Pneumococcal Sequencing Project](https://www.pneumogen.net/gps/)) database updates. 

This tool takes the GPS database's three `.csv` (comma-separated values) source files as input:
- `table1` - metadata
- `table2` - QC
- `table3` - analysis

The tool carries out several operations in the following order:
1. Validation of columns and values in the `.csv` input files
   - The terminal output displays any unexpected or erroneous values
   - For columns that should only contain UPPERCASE strings, all strings will be converted to UPPERCASE strings
   - If there are any critical errors, the tool will terminate its process and will not carry out the subsequent operations
2. Generate `table4` using inferred data based on `table1`, `table3` and reference tables in the `data` directory (i.e. `coordinates.csv`, `manifestations.csv`, `non_standard_ages.csv`, `pcv_introduction_year.csv`, `pcv_valency.csv`, `published_public_names.txt`)
3. Generate [Monocle](https://github.com/sanger-pathogens/monocle)-ready `.csv` files in 3-table format
4. Generate `data.json` for GPS Database Overview (upcoming project)

&nbsp;
## Workflow
![Workflow Diagram](doc/workflow.drawio.svg)

&nbsp;
## Usage
### Basic
1. Clone this repoistory to your machine
2. Install `Python 3`, `Pandas`, `NumPy`, `geopy` with `pip` or create a `conda` environment with these packages
3. Put the GPS database's three `.csv` source files into the directory containing the cloned repoistory
4. Run the following command to validate your input files and generate the output files:
   ```
   python processor.py
   ```

### Optional arguments
- By default, the tool assumes the file names of table1, table2 and table3 to be `table1.csv`, `table2.csv`, `table3.csv` respectively, and the output file name of table4 will be `table4.csv`, these can be changed by using optional arguments; The 3 Monocle-ready `.csv` files will use the file names of table1, table2 and table3 with `_monocle` appended at the end; The data file for GPS Database Overview will always have the file name `data.json`
- Available optional arguments:
  - `-m your_file_name.csv` or `--meta your_file_name.csv`: Override the default table1 (metadata) file name of table1.csv
  - `-q your_file_name.csv` or `--qc your_file_name.csv`: Override the default table2 (qc) file name of table2.csv
  - `-a your_file_name.csv` or `--analysis your_file_name.csv`: Override the default table3 (analysis) file name of table3.csv
  - `-o your_file_name.csv` or `--output your_file_name.csv`: Override the default table4 file name of table4.csv
- Example command using optional arguments:
  ```
  python processor.py -m table1_meta.csv -q table2_qc.csv -a table3_analysis.csv -o table4_inferred.csv
  ```

### Updating Reference Tables
- If you have updated any of the reference tables (i.e. all files in the `data` directory except `api_keys.py`), please create a PR (Pull Request) on this repository. 

### About `api_keys.py`
- Currently, `api_keys.py` is only used to provide Mapbox API key (access token) for geocoding via `geopy` if there is any Country-Region-City combination in `table1` not found in the `coordinates.csv` reference table. For more information on Mapbox API key (access token), please visit [their documentation](https://docs.mapbox.com/help/glossary/access-token/).

&nbsp;
## Requirements & Compatibility
GPS Database requirement:
- GPS1 v4.0+

Tested on:
- [Python](https://www.python.org/) 3.10
- [Pandas](https://pandas.pydata.org/) 1.4.2
- [NumPy](https://numpy.org/) 1.22.4
- [geopy](https://github.com/geopy/geopy) 2.2.0


&nbsp;
## Credits
This project uses Open Source components. You can find the source code of their open source projects along with license information below. I acknowledge and am grateful to these developers for their contributions to open source.

[**Pandas**](https://pandas.pydata.org/)
- Copyright (c) 2008-2011, AQR Capital Management, LLC, Lambda Foundry, Inc. and PyData Development Team. All rights reserved.
- Copyright (c) 2011-2022, Open source contributors.
- License (BSD-3-Clause): https://github.com/pandas-dev/pandas/blob/main/LICENSE

[**NumPy**](https://numpy.org/)
- Copyright (c) 2005-2022, NumPy Developers. All rights reserved.
- License (BSD-3-Clause): https://github.com/numpy/numpy/blob/main/LICENSE.txt

[**geopy**](https://github.com/geopy/geopy)
- © geopy contributors 2006-2018 under the MIT License.
- License (MIT): https://github.com/geopy/geopy/blob/master/LICENSE

[**draw.io / diagrams.net**](https://www.diagrams.net/)
- draw.io is owned and developed by JGraph Ltd, a UK based software company.
- License (Apache License 2.0): https://github.com/jgraph/drawio/blob/dev/LICENSE