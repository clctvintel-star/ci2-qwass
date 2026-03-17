from playwright.sync_api import sync_playwright
import trafilatura
import time
import sys


ARCHIVE_HOSTS = [
    "archive.ph",
    "archive.today",
    "archive.is"
]


def fetch_article(url):

    with sync_playwright() as p:

        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        snapshot_url = None

        for host in ARCHIVE_HOSTS:

            resolver = f"https://{host}/?run=1&url={url}"

            print("Trying resolver:", resolver)

            page.goto(resolver, wait_until="domcontentloaded")

            time.sleep(4)

            current = page.url

            if "/?run=1&url=" not in current:
                snapshot_url = current
                break

        if not snapshot_url:
            raise Exception("Archive resolver did not return snapshot")

        print("Snapshot:", snapshot_url)

        page.goto(snapshot_url, wait_until="domcontentloaded")

        try:
            page.locator("text=Webpage").first.click(timeout=3000)
        except:
            pass

        time.sleep(2)

        html = page.content()

        browser.close()

    text = trafilatura.extract(html)

    return text


if __name__ == "__main__":

    url = sys.argv[1] if len(sys.argv) > 1 else None

    if not url:
        raise Exception("Provide URL")

    text = fetch_article(url)

    print("\n\nARTICLE TEXT PREVIEW\n")
    print(text[:2000])
