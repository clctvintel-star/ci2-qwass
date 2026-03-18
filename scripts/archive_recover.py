import requests
import sys
import re
from bs4 import BeautifulSoup

ARCHIVE_SITES = [
    "https://archive.ph",
    "https://archive.is",
    "https://archive.today",
    "https://archive.md",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}


def find_snapshot(original_url):

    for site in ARCHIVE_SITES:

        try:
            search_url = f"{site}/{original_url}"
            print(f"Trying resolver: {search_url}")

            r = requests.get(search_url, headers=HEADERS, timeout=30)

            if r.status_code != 200:
                continue

            soup = BeautifulSoup(r.text, "html.parser")

            links = soup.find_all("a", href=True)

            for link in links:

                href = link["href"]

                if re.match(r"^/[A-Za-z0-9]{5,}$", href):
                    return site + href

        except Exception as e:
            print("resolver error:", e)

    return None


def extract_article(snapshot_url):

    print("Fetching snapshot:", snapshot_url)

    r = requests.get(snapshot_url, headers=HEADERS, timeout=30)

    soup = BeautifulSoup(r.text, "html.parser")

    paragraphs = soup.find_all("p")

    text = "\n\n".join(p.get_text(strip=True) for p in paragraphs)

    return text


def main():

    if len(sys.argv) < 2:
        print("Usage: python archive_recover.py URL")
        sys.exit(1)

    url = sys.argv[1]

    snapshot = find_snapshot(url)

    if not snapshot:
        print("No snapshot found.")
        sys.exit(1)

    print("Snapshot found:", snapshot)

    article = extract_article(snapshot)

    print("\nARTICLE TEXT PREVIEW\n")
    print(article[:2000])


if __name__ == "__main__":
    main()
