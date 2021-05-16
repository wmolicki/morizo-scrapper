# morizon.pl scrapper

## Installation

Requires: python3.8+

Install dependencies with:
`pip install -r requirements.txt`

## Usage

Run `python main.py -h` for displaying help along with flag defaults

Example usage:

```
python main.py --url="https://www.morizon.pl/mieszkania/najnowsze/gdansk/?ps%5Bext_prp%5D%5Bdate_filter%5D=added_at_7&ps%5Bdate_filter%5D=30&ps%5Bowner%5D%5B0%5D=4&ps%5Bwith_price%5D=1&ps%5Bwith_photo%5D=1&ps%5Bmarket_type%5D%5B0%5
D=1&ps%5Bmarket_type%5D%5B1%5D=2" -o my_results.csv
```

By default downloaded pages are cached in cached_pages directory. 
This cache can be cleared by adding `--cache-clear` flag