# GPS Database Processor - Work-In-Progress

GPS Database Processor is an all-in-one tool for processing GPS database updates. 

This tool takes the GPS database's three `.csv` (comma-separated values) source files as input:
- `table1.csv` - metadata
- `table2.csv` - QC
- `table3.csv` - analysis

The tool carries out several operations in the following order:
1. Validation of columns and values in the `.csv` files
   - The terminal output displays any unexpected or erroneous values
   - If there are any critical errors, the tool will terminate its process and will not carry out the subsequent operations
2. Generate `table4.csv` based on inferred data
3. Generate [Monocle](https://github.com/sanger-pathogens/monocle)-ready CSV files for all tables
   - Removing non-public data
   - Adding column(s) required by Monocle
4. Generate `data.json` for GPS Database Overview (upcoming project)


&nbsp;
## Usage
TBC


&nbsp;
## Requirements & Compatibility
Python script requirements:
- [Python](https://www.python.org/) 3.10
- [Pandas](https://pandas.pydata.org/) 1.4.0
- [geopy](https://github.com/geopy/geopy) 2.2.0

GPS Database requirement:
- TBC


&nbsp;
## Credits
This project uses Open Source components. You can find the source code of their open source projects along with license information below. I acknowledge and am grateful to these developers for their contributions to open source.

[**Pandas**](https://pandas.pydata.org/)
- Copyright (c) 2008-2011, AQR Capital Management, LLC, Lambda Foundry, Inc. and PyData Development Team. All rights reserved.
- Copyright (c) 2011-2022, Open source contributors.
- License (BSD-3-Clause): https://github.com/pandas-dev/pandas/blob/main/LICENSE

[**geopy**](https://github.com/geopy/geopy)
- © geopy contributors 2006-2018 (see AUTHORS) under the MIT License.
- License (MIT): https://github.com/geopy/geopy/blob/master/LICENSE