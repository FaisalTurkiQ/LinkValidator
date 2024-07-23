# LinkValidator

A Python script to check and update links in a DataFrame, convert `http` to `https`, remove specific parameters, and generate a PDF report summarizing the results.

## Features

- Load links from a CSV or XLSX file.
- Convert `http` links to `https`.
- Remove `igshid` parameter from Instagram links.
- Check the status of each link.
- Generate a PDF report with the results.

## Requirements

- Python 3.6+
- pandas
- requests
- reportlab
- openpyxl

## Installation

1. Clone the repository:
   git clone https://github.com/yourusername/link_checker.git
   cd LinkValidator
   
3. Install the required packages:
pip install -r requirements.txt
Usage
Place your CSV or XLSX file in the project directory.

Modify the file_path, sheet_name, and column_name variables in the script to match your file and column names.

Run the script:
The script will update the links, check their status, and generate a PDF report in the project directory.





