# Properties Scrapper

This project extracts property information published on `www.arrendamientossantafe.com` through web scraping using `Beautiful Soup` and `Requests` libraries.

The process handles querying the rental and sales pages to extract the main information from each of the properties listed on those pages. It then iterates over each property to extract detailed information. Additionally, it uses the `Novatim` API to construct the approximate address of the property based on latitude and longitude.

After extracting the information, it is loaded into two tables in a `BigQuery` storage.

## Project Structure

```bash
/scrapper/
│
├── main.py             # Main script to execute the scrapper.
├── modules/            # Folder with the modules of the scrapper.
│   ├── __init__.py     # Empty file to python package recognition.
│   ├── extract.py      # Script to extract properties data.
│   ├── transform.py    # Script to transform data.
│   └── load.py         # Script to load data to bigquery.
└── requirements.txt    # Required PIP libraries.
```