# GPS Database Validator (Work-in-Progress)

This tool is used to check whether your [Global Pneumococcal Sequencing Project](https://www.pneumogen.net/gps/) (GPS) Database only contains expected values or values in the expected formats. 


&nbsp;
## Usage
To check your database,  put the database in the same directory/folder as the script and run the following command (change `database.db` to the file name of the database):
```
python validator.py database.db
```


&nbsp;
## Requirements & Compatibility
Python script requirements:
- [Python](https://www.python.org/) 3.10
- [Pandas](https://pandas.pydata.org/) 1.4.0

GPS Database requirement:
- GPS1 v3.4


&nbsp;
## Credits
This project uses Open Source components. You can find the source code of their open source projects along with license information below. I acknowledge and am grateful to these developers for their contributions to open source.

[**Pandas**](https://pandas.pydata.org/)
- Copyright (c) 2008-2011, AQR Capital Management, LLC, Lambda Foundry, Inc. and PyData Development Team. All rights reserved.
- Copyright (c) 2011-2022, Open source contributors.
- License (BSD-3-Clause): https://github.com/pandas-dev/pandas/blob/main/LICENSE