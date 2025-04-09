# Alberta Public Well Data Pipeline
Author: [Matt Nelson](https://mnelson.ca)

An example data pipeline for downloading, processing, and normalizing public well data from Alberta Energy Regulator (AER) and Petrinex sources. 

## Overview

This project automates the collection and processing of public well data from Alberta, including:
- AER ST1 Well License data
- AER ST37 Well data
- Petrinex Production Volume data

It is not meant to create a reference quality dataset, many verifications and improvements could likely be made, but rather to serve as a starting point for data processing and analysis.

Some data cleaning has been completed - including selecting and renaming columns, and imputing of missing Well License information where necessary.

## Installation

### Prerequisites

- uv (https://docs.astral.sh/uv/getting-started/installation/)


## Setup and Usage

Run the pipeline. On first run, uv will set up a virtual environment and install dependencies:

```bash
uv run main.py
```

### Command Line Options

- `--skip-download`: Skip downloading data and use existing files in the `data/raw` directory
- `--run-profiles`: Generate detailed profile reports (slower processing). These are not required for the pipeline to run, but they can be useful for data quality analysis.

## Project Structure

```
ab-public-well-data/
├── data/               # Data storage
│   ├── raw/            # Raw downloaded data
│   ├── profiles/       # Data profile reports (if generated)
│   └── staging/        # Intermediate processed data
├── output/             # Final output data
├── src/                # Source code
│   ├── downloader.py   # Data download functions
│   ├── loader.py       # Data loading and validation
│   ├── normalize.py    # Data normalization functions
│   └── output.py       # Output generation
├── main.py             # Main pipeline script
└── README.md           # This file
```

## License

This project is licensed under the MIT License - see below for details:

```
MIT License

Copyright (c) 2025

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.