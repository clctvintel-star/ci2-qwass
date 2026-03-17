from playwright.sync_api import sync_playwright
import trafilatura
import time
import json

ARCHIVE_HOSTS = [
    "archive.ph",
    "archive.today",
    "archive.is"
]


def find_snapshot(page, url):

    for host in ARCHIVE_HOSTS:

        search_url = f"https://{host}/search/?q={url}"

        print(f"Searching {search_url}")

        page.goto(search_url, wait_until="domcontentloaded")

        try:
            page.wait_for_selector("a[href^='https://archive.']", timeout=8000)
        except:
            continue

        links = page.locator("a[href^='https://archive.']")

        if links.count() == 0:
            continue

        snapshot = links.first.get_attribute("href")

        if snapshot:
            return snapshot

    return None


def open_snapshot(page, snapshot_url):

    print("Opening snapshot:", snapshot_url)

    page.goto(snapshot_url, wait_until="domcontentloaded")

    # click webpage tab if present
    try:
        page.locator("text=Webpage").first.click(timeout=3000)
    except:
        pass

    time.sleep(2)

    html = page.content()

    return html


def extract_article(html):

    text = trafilatura.extract(html)

    return text


def fetch_article(url):

    with sync_playwright() as p:

        browser = p.chromium.launch(headless=True)

        page = browser.new_page()

        snapshot = find_snapshot(page, url)

        if not snapshot:
            browser.close()
            raise Exception("No snapshot found")

        html = open_snapshot(page, snapshot)

        browser.close()

    article_text = extract_article(html)

    return {
        "url": url,
        "snapshot": snapshot,
        "text": article_text
    }


if __name__ == "__main__":

    url = "https://www.bloomberg.com/news/features/2026-02-05/how-two-sigma-founder-s-divorce-fight-hangs-over-hedge-fund-s-future"

    result = fetch_article(url)

    with open("article.json", "w") as f:
        json.dump(result, f, indent=2)

    print("\nSUCCESS\n")
    print(result["text"][:2000])
