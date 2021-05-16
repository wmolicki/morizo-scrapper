import argparse
import codecs
import dataclasses
import shutil
import sys

from typing import Optional
import hashlib

import requests
import os

from bs4 import BeautifulSoup

from concurrent.futures import ThreadPoolExecutor, wait
from threading import Lock

from dataclass_csv import DataclassWriter
from dateparser import DateDataParser

URL = "https://www.morizon.pl/mieszkania/najnowsze/gdansk/"
QS = (
    "?ps[ext_prp][date_filter]=added_at_7"
    "&ps[date_filter]=30"
    "&ps[owner][0]=1"
    "&ps[owner][1]=4"
    "&ps[owner][2]=2"
    "&ps[owner][3]=128"
    "&ps[with_price]=1"
    "&ps[with_photo]=0"
    "&ps[market_type][0]=1"
    "&ps[market_type][1]=2"
)
HTML_PAGE_FILENAME = "resp.html"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36"


@dataclasses.dataclass
class Result:
    short_title: str
    url: str
    short_description: str

    title: Optional[str] = None
    price: Optional[float] = None
    price_per_m2: Optional[float] = None
    rooms: Optional[int] = None
    area: Optional[float] = None

    date_added: Optional[str] = None
    date_built: Optional[int] = None

    floor: Optional[int] = None
    floors: Optional[int] = None

    lat: Optional[float] = None
    lng: Optional[float] = None


def fetch_results(url, results, lock) -> None:
    print(f'Fetching page: {url[len(url)-10:]}')
    html = get_html_contents(url=url)

    soup = BeautifulSoup(html, features="html.parser")

    main_box = soup.find_all("div", class_="mainBox")[0]
    result_section = main_box.find_next("section")
    result_rows = result_section.find_all("div", class_="single-result")

    # filter out last row as its an ad
    for row in result_rows[:-1]:
        content = row.find_next("section")
        header = content.find_next("header")
        url = header.a.attrs["href"].strip()
        title = header.find_next("h2", class_="single-result__title").text.strip()
        short_description = content.find_next(
            "div", class_="description"
        ).p.text.strip()
        with lock:
            results.append(
                Result(url=url, short_title=title, short_description=short_description)
            )

    print(f'Page: {url[len(url) - 10:]} completed.')


def main():
    parser = argparse.ArgumentParser(description='Scrapper for morizon.pl')
    parser.add_argument('--url', dest='url', action='store', default=f"{URL}?{QS}",
                        help='url with query filters for scrapping (should be first page of results)')
    parser.add_argument('-o', dest='result_file', action='store', default='result.csv',
                        help='destination file (default=result.csv)')
    parser.add_argument('--cache-clear', action='store_true', help='clear cached_pages folder')
    args = parser.parse_args()

    lock = Lock()

    url = args.url
    result_file = args.result_file
    cache_clear = args.cache_clear

    if cache_clear:
        shutil.rmtree('cached_pages')

    html = get_html_contents(url=url)

    soup = BeautifulSoup(html, features="html.parser")
    main_box = soup.find_all("div", class_="mainBox")[0]

    # use this for debugging
    # get_details(result=Result(url="https://www.morizon.pl/oferta/sprzedaz-mieszkanie-gdansk-jasien-potegowska-35m2-mzn2038736185", short_title="", short_description=""), ddp=ddp, lock=lock)

    footer = main_box.find_next('footer')
    pagination_ul = footer.ul
    if pagination_ul:
        pages = int(pagination_ul.find_all('li')[-2].text.strip())
    else:
        pages = 1

    pool = ThreadPoolExecutor(max_workers=8)

    results = []
    tasks = []

    for page_num in range(1, pages+1):
        tasks.append(pool.submit(fetch_results, f'{url}&page={page_num}', results, lock))

    # wait a minute until completed
    wait(tasks, timeout=60)

    pool.map(get_details, results, timeout=300)

    with codecs.open(result_file, "w", encoding="utf-8") as f:
        w = DataclassWriter(f, results, Result)
        w.write()

    print(f'saved results for {url} to {result_file}')


def get_details(result: Result) -> Result:
    print(f"Fetching details for {result.url[len(result.url)-40:]}")

    html = get_html_contents(url=result.url)
    soup = BeautifulSoup(html, features="html.parser")
    content_box = soup.find_all("div", class_="contentBox")[0]
    article = content_box.article

    title = " ".join(
        [
            i.strip()
            for i in article.find_next("div", class_="summaryLocation").text.split("\n")
            if i
        ]
    )
    result.title = title

    params_ul = article.find_next("ul", class_="paramIcons")
    price = (
        params_ul.find_next("li", class_="paramIconPrice")
        .text.replace("zł", "")
        .replace("Cena", "")
        .replace(" ", "")
        .replace(",", ".")
        .strip()
    )
    price = cast_or_none(float, price)
    price_per_m2_elem = params_ul.find_next("li", class_="paramIconPriceM2")
    if price_per_m2_elem:
        price_per_m2 = (
            params_ul.find_next("li", class_="paramIconPriceM2")
            .text.replace("zł", "")
            .replace("Cena za m²", "")
            .replace(" ", "")
            .replace(",", ".")
            .strip()
        )
        price_per_m2 = cast_or_none(float, price_per_m2)
        result.price_per_m2 = price_per_m2
    area = (
        params_ul.find_next("li", class_="paramIconLivingArea")
        .text.replace("m²", "")
        .replace("Powierzchnia", "")
        .replace(" ", "")
        .replace(",", ".")
        .strip()
    )
    area = cast_or_none(float, area)
    rooms = (
        params_ul.find_next("li", class_="paramIconNumberOfRooms")
        .text.replace("Pokoje", "")
        .replace(" ", "")
        .strip()
    )
    result.price = price
    result.area = area
    result.rooms = rooms

    # PARSING SECTION: Informacje szczegółowe
    property_content_section = article.find_next("section", class_="propertyContent")
    property_params = property_content_section.find_next(
        "section", class_="propertyParams"
    ).section
    details_table = property_params.table
    details_props = _parse_table(table=details_table)

    date_added = details_props.get("opublikowano")
    if date_added:
        try:
            ddp = DateDataParser(languages=['pl'])
            date_added = ddp.get_date_data(date_added).date()
        except (ValueError, TypeError):
            pass

    floor = get_floor(details_props.get("piętro", ""))
    floors = cast_or_none(int, details_props.get("liczba pięter"))
    result.floors = floors
    result.date_added = date_added
    result.floor = floor

    # PARSING SECTION: Budynek
    try:
        building_table = property_params.find_all("table")[1]
        building_props = _parse_table(table=building_table)
        date_built = cast_or_none(int, building_props.get("rok budowy"))
        result.date_built = date_built
    except IndexError:
        pass

    # PARSING MAP LAT/LON
    google_map = article.find_next("div", id="property-map")
    if google_map:
        lat = cast_or_none(float, google_map.attrs["data-lat"].replace(",", "."))
        lng = cast_or_none(float, google_map.attrs["data-lng"].replace(",", "."))
        result.lat = lat
        result.lng = lng

    print(f"Finished {result.url[:40]}")

    return result


def _parse_table(table):
    rows = table.find_all("tr")
    props = {}
    for tr in rows:
        th = tr.find_all("th")
        td = tr.find_all("td")
        row = {
            h.text.replace(":", "").strip().lower(): d.text.strip().lower()
            for h, d in zip(th, td)
        }
        props.update(row)
    return props


def cast_or_none(type_, val):
    try:
        return type_(val)
    except (TypeError, ValueError):
        return None


def get_floor(val: str) -> Optional[int]:
    if "/" in val:
        val = val.split("/")[0].strip()

    if "parter" in val:
        return 0
    if "pierwsz" in val:
        return 1
    if "drug" in val:
        return 2
    if "trz" in val:
        return 3
    if "czwa" in val:
        return 4
    if "pią" in val:
        return 5
    if "szó" in val:
        return 6
    if "sió" in val:
        return 7
    if "ósm" in val:
        return 8
    if "dziew" in val:
        return 9
    if "dzies" in val:
        return 10

    return cast_or_none(int, val)


def get_html_contents(url) -> str:
    if not os.path.exists("cached_pages"):
        os.makedirs("cached_pages")
    filename = f'cached_pages/{hashlib.md5(url.encode("utf-8")).hexdigest()}.html'
    if not os.path.exists(filename):
        response = requests.get(url, headers={"User-Agent": USER_AGENT})
        with open(filename, "wb") as f:
            f.write(response.text.encode("utf-8"))
    with open(filename, "rb") as f:
        html = f.read().decode("utf-8")
    return html


if __name__ == "__main__":
    main()
