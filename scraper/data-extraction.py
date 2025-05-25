import json
import logging
import requests
import os
from bs4 import BeautifulSoup, Tag
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen

# Logging configuration
logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")

BASE_URL = "https://datos.madrid.es"
MAIN_URL = (
    "https://datos.madrid.es/portal/site/egob/menuitem.c05c1f754a33a9fbe4b2e4b284f1a5a0/"
    "?vgnextoid=33cb30c367e78410VgnVCM1000000b205a0aRCRD"
    "&vgnextchannel=374512b9ace9f310VgnVCM100000171f5a0aRCRD"
)


def get_last_data() -> tuple[str, str]:
    """
    Retrieves the latest year and month that has been processed.
    Returns:
        tuple: (last_year, last_month) if data exists, otherwise ("", "").
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    json_path = os.path.join(script_dir, "files-data.json")
    if not os.path.exists(json_path):
        logging.error("The JSON file does not exist.")
        return "", ""

    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not data:
        logging.error("No data found in the JSON file.")
        return "", ""

    last_entry = data[0]
    last_year = last_entry.get("year", "")
    last_month = last_entry.get("month", "")

    return last_year, last_month


def get_data(url: str, last_year: str, last_month: str) -> None:
    """
    Extracts ZIP (CSV) file links from the given URL and saves them to a local JSON file.

    If `last_year` and `last_month` are provided, stops processing at that point.
    If both are empty, processes all available data. New links are added at the top and duplicates are avoided.

    Parameters:
        url (str): URL of the webpage containing the ZIP file links.
        last_year (str): Last processed year. If empty, all years are processed.
        last_month (str): Last processed month. If empty, all months are processed.

    Raises:
        ValueError: If key HTML elements are missing from the page.
        TypeError: If an unexpected element type is found in the HTML.
        requests.RequestException: If the URL cannot be accessed.

    Returns:
        None
    """

    try:
        request = Request(url, headers={"User-Agent": "Mozilla/5.0"})
        html = urlopen(request).read()
    except requests.RequestException as e:
        logging.error(f"Error accessing the main URL: {e}")
        raise

    soup = BeautifulSoup(html, "html.parser")
    links = []
    logging.info("Retrieving CSV file links...")

    main = soup.find("ul", {"class": "asociada-list trancateList docs"})
    if not isinstance(main, Tag):
        raise ValueError("Main document list container not found.")

    years = main.find_all("li", {"class": "asociada-item"}, recursive=False)
    if not years:
        raise ValueError("No year elements found.")

    stop_processing = False

    for year in years:
        if not isinstance(year, Tag):
            raise TypeError("Year element is not a valid Tag.")

        year_text = year.find("p", {"class": "info-title"})
        if year_text is None:
            raise ValueError("Year title not found.")
        year_value = year_text.get_text(strip=True)

        months = year.find_all("li", {"class": "asociada-item"})
        if not months:
            raise ValueError(f"No months found for year {year_value}.")

        for m in months:
            if not isinstance(m, Tag):
                logging.warning(
                    f"Month element is not a valid Tag in year {year_value}. Skipping."
                )
                continue

            month_text = m.find("p", {"class": "info-title"})
            if month_text is None:
                continue
            month_value = month_text.get_text(strip=True)

            # Only stop if both last_year and last_month are specified and matched
            if last_year and last_month and year_value == last_year and month_value == last_month:
                logging.info("The rest of the data has already been processed. Exiting.")
                stop_processing = True
                break

            links_li = m.find("li", {"class": "asociada-item"})
            if links_li is None or not isinstance(links_li, Tag):
                logging.warning(
                    f"Links container not found in {year_value} {month_value}. Skipping."
                )
                continue

            zip_link = links_li.find("a", {"class": "asociada-link ico-zip"})
            if zip_link is None or not isinstance(zip_link, Tag):
                logging.warning(
                    f"ZIP link not found in {year_value} {month_value}. Skipping."
                )
                continue

            href = zip_link.get("href")
            if not isinstance(href, str):
                logging.warning(
                    f"href attribute not found in ZIP link for {year_value} {month_value}. Skipping."
                )
                continue

            full_url = urljoin(BASE_URL, href)
            logging.info(f"{year_value} {month_value}: {full_url}")
            links.append({"year": year_value, "month": month_value, "link": full_url})

        if stop_processing:
            break

    # Save to JSON
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_path = os.path.join(script_dir, "files-data.json")
    if os.path.exists(output_path):
        with open(output_path, "r", encoding="utf-8") as f:
            existing_links = json.load(f)
        all_links = links + [item for item in existing_links if item not in links]
    else:
        all_links = links
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(all_links, f, ensure_ascii=False, indent=2)

'''
def download_csv(url: str, dest_folder: str = "csv_files") -> str:
    """Descarga un archivo CSV desde la URL especificada."""
    os.makedirs(dest_folder, exist_ok=True)
    filename = os.path.join(dest_folder, os.path.basename(urlparse(url).path))
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        with open(filename, "wb") as f:
            f.write(resp.content)
        logging.info(f"Descargado: {filename}")
        return filename
    except requests.RequestException as e:
        logging.error(f"Error descargando {url}: {e}")
        return ""
'''


def main():
    last_year, last_month = get_last_data()
    get_data(MAIN_URL, last_year, last_month)



if __name__ == "__main__":
    main()
