import asyncio
import json
import logging
import os
import platform
import signal
import sys
from collections import deque
from dataclasses import dataclass, asdict, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Deque
from urllib.parse import urlencode

import aiofiles
from bs4 import BeautifulSoup
from curl_cffi.requests import AsyncSession
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure stdout/stderr encoding for Windows
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

# Constants
EBAY_DOMAIN = {
    "GB": "https://www.ebay.co.uk"
}


@dataclass
class ProductDetails:
    """Data class for storing product information."""
    url: str
    title: Optional[str] = None
    subtitle: Optional[str] = None
    current_price: Optional[str] = None
    was_price: Optional[str] = None
    discount: Optional[str] = None
    availability: Optional[str] = None
    sold_count: Optional[str] = None
    shipping: Optional[str] = None
    location: Optional[str] = None
    returns: Optional[str] = None
    condition: Optional[str] = None
    brand: Optional[str] = None
    type: Optional[str] = None
    store_info: Optional[Dict[str, str]] = field(default_factory=lambda: {
        "name": "",
        "feedback": "",
        "sales": ""
    })


class FileHandler:
    """Handles file operations for the scraper."""

    @staticmethod
    async def save_to_file(filename: str, data: Dict, is_backup: bool = False):
        """Save data to file with proper error handling."""
        try:
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            temp_file = f"{filename}.temp"

            async with aiofiles.open(temp_file, "w", encoding="utf-8") as f:
                await f.write(json.dumps(data, indent=2, ensure_ascii=False))
            os.replace(temp_file, filename)

            if not is_backup:
                logging.info(f"Saved data to {filename}")

        except Exception as e:
            logging.error(f"Error saving to file {filename}: {str(e)}")
            await FileHandler.create_emergency_backup(filename, data)

    @staticmethod
    async def create_emergency_backup(filename: str, data: Dict):
        """Create emergency backup in case of save failure."""
        try:
            emergency_file = f"{filename}.emergency_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            async with aiofiles.open(emergency_file, "w", encoding="utf-8") as f:
                await f.write(json.dumps(data, indent=2, ensure_ascii=False))
            logging.info(f"Created emergency backup: {emergency_file}")
        except Exception as e:
            logging.error(f"Emergency backup failed: {str(e)}")


class HTMLParser:
    """Handles HTML parsing operations."""

    @staticmethod
    def extract_store_info(soup: BeautifulSoup) -> Dict[str, str]:
        """Extract seller/store information from the page."""
        store_info = {"name": "", "feedback": "", "sales": ""}

        try:
            if store_header := soup.select_one("div.x-store-information__header"):
                if store_name := store_header.select_one("span.ux-textspans--BOLD"):
                    store_info["name"] = store_name.text.strip()

                if highlights := store_header.select_one("h4.x-store-information__highlights"):
                    for span in highlights.select("span.ux-textspans"):
                        store_info["feedback"] = span.text.strip()
                        break

                    sales_spans = highlights.select("span.ux-textspans--SECONDARY")
                    if len(sales_spans) >= 2:
                        store_info["sales"] = f"{sales_spans[0].text.strip()} {sales_spans[1].text.strip()}"

        except Exception as e:
            logging.error(f"Error extracting store information: {str(e)}")

        return store_info

    @staticmethod
    def extract_shipping_info(shipping_div: Optional[BeautifulSoup]) -> Tuple[Optional[str], Optional[str]]:
        """Extract shipping and location information."""
        shipping = None
        location = None

        if shipping_div:
            if shipping_row := shipping_div.select_one(
                "div.ux-layout-section__row div.ux-labels-values-with-hints[data-testid='ux-labels-values-with-hints'] "
                "div.ux-labels-values--shipping"
            ):
                shipping_text_parts = []

                if shipping_section := shipping_row.select_one("div.ux-labels-values__values-content"):
                    for span in shipping_section.find_all("span", class_="ux-textspans"):
                        if span.text.strip() and 'See details' not in span.text:
                            text = span.text.strip()
                            if text and text not in ['.', '&nbsp;', ' ']:
                                shipping_text_parts.append(text)

                    shipping = " ".join(shipping_text_parts).strip()
                    if "Located in:" in shipping:
                        shipping = shipping.split("Located in:")[0].strip()
                    if shipping.endswith('.'):
                        shipping = shipping[:-1]

                if location_span := shipping_row.select_one(
                    "div.ux-labels-values__values-content span.ux-textspans--SECONDARY"
                ):
                    location_text = location_span.text.strip()
                    if "Located in:" in location_text:
                        location = location_text.split("Located in:")[-1].strip()

        return shipping, location


class EbayRealTimeScraper:
    """Main scraper class for eBay products."""

    def __init__(
        self,
        domain: str,
        max_retries: int = 3,
        max_concurrent_requests: int = 3,
        batch_size: int = 50,
        max_items: Optional[int] = None,
    ):
        self.setup_credentials()
        self.setup_scraper_config(domain, max_retries, max_concurrent_requests, batch_size, max_items)
        self.initialize_state()
        self.setup_signal_handlers()

    def setup_credentials(self):
        """Set up proxy credentials."""
        self.proxy_host = "network.joinmassive.com:65534"
        self.username = os.getenv("PROXY_USERNAME")
        self.password = os.getenv("PROXY_PASSWORD")

        if not self.username or not self.password:
            raise ValueError("Proxy credentials not found in environment variables")

    def setup_scraper_config(self, domain: str, max_retries: int, max_concurrent_requests: int,
                           batch_size: int, max_items: Optional[int]):
        """Configure scraper settings."""
        self.domain = domain
        self.proxy_auth = f"{self.username}-country-{self.domain}:{self.password}"
        self.base_search_url = EBAY_DOMAIN.get(domain)

        if not self.base_search_url:
            raise ValueError(f"Invalid domain identifier. Valid options are: {', '.join(EBAY_DOMAIN.keys())}")

        self.base_search_url += "/sch/i.html?"
        self.max_retries = max_retries
        self.max_concurrent_requests = max_concurrent_requests
        self.batch_size = batch_size
        self.max_items = max_items

        # Configuration constants
        self.page_load_timeout = 30
        self.retry_delay = 5
        self.retry_statuses = {408, 429, 500, 502, 503, 504}
        self.min_content_length = 50000
        self.required_elements = {
            "search_page": ["ul.srp-results", "li.s-item"],
            "product_page": ["div.x-item-title", "div.x-price-section"],
        }

    def initialize_state(self):
        """Initialize scraper state variables."""
        self.scraped_urls: Set[str] = set()
        self.products: List[Dict] = []
        self.product_buffer: Deque[Dict] = deque(maxlen=self.batch_size)
        self.total_saved = 0
        self.current_output_file = None
        self.last_saved_products = []
        self.semaphore = asyncio.Semaphore(self.max_concurrent_requests)
        self.items_scraped = 0
        self.save_count = 0
        self.backup_interval = 20
        self.saved_product_ids = set()

    def setup_signal_handlers(self):
        """Set up handlers for system signals."""
        try:
            for sig in (signal.SIGTERM, signal.SIGINT):
                signal.signal(sig, self.handle_shutdown)
        except AttributeError:
            pass

    def handle_shutdown(self, signum, frame):
        """Handle system shutdown signals."""
        logging.info("Shutdown signal received. Saving remaining data...")
        if self.product_buffer and self.current_output_file:
            loop = asyncio.get_event_loop()
            loop.run_until_complete(self.force_save_buffer())
        logging.info("Shutdown complete")
        sys.exit(0)

    def _get_proxy_config(self) -> Dict[str, str]:
        """Get proxy configuration."""
        return {"https": f"http://{self.proxy_auth}@{self.proxy_host}"}

    def validate_page_content(self, html_content: str, page_type: str) -> Tuple[bool, str]:
        """Validate the content of scraped pages."""
        if not html_content or len(html_content) < self.min_content_length:
            return False, "Content too short"

        soup = BeautifulSoup(html_content, "lxml")
        error_texts = [
            "Sorry, the page you requested was not found",
            "This listing was ended",
            "Security Measure",
            "Robot Check"
        ]

        for error in error_texts:
            if error.lower() in html_content.lower():
                return False, f"Error page detected: {error}"

        for selector in self.required_elements[page_type]:
            if not soup.select(selector):
                return False, f"Missing required element: {selector}"

        return True, "Page content valid"

    async def _make_request(self, session: AsyncSession, url: str, page_type: str) -> Tuple[int, Optional[str]]:
        """Make HTTP request with retry logic."""
        retries = 0
        last_error = None

        while retries < self.max_retries:
            try:
                async with self.semaphore:
                    response = await session.get(
                        url,
                        proxies=self._get_proxy_config(),
                        impersonate="chrome124",
                        timeout=self.page_load_timeout,
                    )

                    if response.status_code == 200:
                        content = response.text
                        is_valid, message = self.validate_page_content(content, page_type)

                        if is_valid:
                            return response.status_code, content
                        else:
                            logging.warning(f"Attempt {retries + 1}: Invalid content: {message}")
                            if "Robot Check" in message or "Security Measure" in message:
                                await asyncio.sleep(self.retry_delay * (retries + 1))
                                retries += 1
                                continue
                    elif response.status_code in self.retry_statuses:
                        logging.warning(f"Attempt {retries + 1}: Got status {response.status_code} for {url}")
                        await asyncio.sleep(self.retry_delay * (retries + 1))
                        retries += 1
                        continue
                    else:
                        return response.status_code, None

            except Exception as e:
                last_error = str(e)
                logging.error(f"Attempt {retries + 1} failed for {url}: {last_error}")

                if "timeout" in last_error.lower() or "connection" in last_error.lower():
                    await asyncio.sleep(self.retry_delay * (retries + 1))
                    retries += 1
                    continue
                else:
                    return None, None

            retries += 1
            await asyncio.sleep(self.retry_delay * retries)

        logging.error(f"All retry attempts failed for {url}. Last error: {last_error}")
        return None, None

    def extract_product_urls(self, soup: BeautifulSoup) -> List[str]:
        """Extract product URLs from search results page."""
        urls = []
        links = soup.select("a.s-item__link")

        for link in links:
            url = link.get("href", "")
            if url and "itm/" in url:
                try:
                    item_id = url.split("itm/")[1].split("?")[0]
                    clean_url = f"{self.base_search_url.split('/sch')[0]}/itm/{item_id}"
                    if clean_url not in self.scraped_urls:
                        urls.append(clean_url)
                except IndexError:
                    continue

        return urls

    def extract_product_details(self, soup: BeautifulSoup, url: str) -> ProductDetails:
        """Extract all product details from product page."""
        details = ProductDetails(url=url)

        try:
            details.store_info = HTMLParser.extract_store_info(soup)

            # Extract title and subtitle
            if title_div := soup.select_one("div.x-item-title"):
                if title := title_div.select_one("h1.x-item-title__mainTitle span"):
                    details.title = title.text.strip()
                if subtitle := title_div.select_one("div.x-item-title__subTitle span"):
                    details.subtitle = subtitle.text.strip()

            # Extract price information
            if price_section := soup.select_one("div.x-price-section"):
                if current_price := price_section.select_one("div.x-price-primary span"):
                    details.current_price = current_price.text.strip()
                if was_price := price_section.select_one("span.ux-textspans--STRIKETHROUGH"):
                    details.was_price = was_price.text.strip()

                # Extract discount
                discount = None
                if emphasis_discount := price_section.select_one("span.ux-textspans--EMPHASIS"):
                    discount = emphasis_discount.text.strip()
                elif secondary_discount := price_section.select_one("span.ux-textspans--SECONDARY"):
                    discount = secondary_discount.text.strip()

                if discount:
                    import re
                    if percentage_match := re.search(r'(\d+)%', discount):
                        details.discount = percentage_match.group(1) + '%'

            # Extract availability and sold count
            if quantity_div := soup.select_one("div.x-quantity__availability"):
                spans = quantity_div.select("span.ux-textspans")
                if len(spans) >= 1:
                    details.availability = spans[0].text.strip()
                if len(spans) >= 2:
                    details.sold_count = spans[1].text.strip()

            # Extract shipping and location
            if shipping_div := soup.select_one("div.d-shipping-minview"):
                details.shipping, details.location = HTMLParser.extract_shipping_info(shipping_div)

            # Extract returns information
            if returns_div := soup.select_one("div.x-returns-minview"):
                if returns_section := returns_div.select_one("div.ux-labels-values__values-content"):
                    returns_text_parts = []
                    spans = returns_section.find_all("span", class_="ux-textspans")

                    for span in spans:
                        if (span.text.strip() and
                            'clipped' not in span.get('class', []) and
                            'See details' not in span.text):

                            text = span.text.strip()
                            if text and text not in ['.', '&nbsp;', ' ']:
                                if 'ux-textspans--EMPHASIS' in span.get('class', []):
                                    text = f"{text}"
                                returns_text_parts.append(text)

                    returns_text = " ".join(returns_text_parts).strip()
                    returns_text = " ".join(returns_text.split())
                    returns_text = returns_text.replace(" .", ".")
                    details.returns = returns_text

            # Extract condition, brand, and type
            if condition_span := soup.select_one("div.x-item-condition-max-view .ux-section__item > span.ux-textspans"):
                details.condition = condition_span.text.strip().split(".")[0] + "."

            if (brand_dl := soup.select_one("dl.ux-labels-values--brand")) and (brand_value := brand_dl.select_one("dd .ux-textspans")):
                details.brand = brand_value.text.strip()
            elif (brand_div := soup.select_one("div.ux-labels-values--brand")) and (brand_value := brand_div.select_one(".ux-labels-values__values .ux-textspans")):
                details.brand = brand_value.text.strip()

            if (type_dl := soup.select_one("dl.ux-labels-values--type")) and (type_value := type_dl.select_one("dd .ux-textspans")):
                details.type = type_value.text.strip()
            elif (type_div := soup.select_one("div.ux-labels-values--type")) and (type_value := type_div.select_one(".ux-labels-values__values .ux-textspans")):
                details.type = type_value.text.strip()

        except Exception as e:
            logging.error(f"Error extracting details from {url}: {str(e)}")

        return details

    async def scrape_product(self, session: AsyncSession, url: str) -> Optional[Dict]:
        """Scrape individual product page."""
        try:
            status_code, html_content = await self._make_request(session, url, "product_page")

            if not html_content:
                logging.error(f"Failed to load product page properly: {url}")
                return None

            soup = BeautifulSoup(html_content, "lxml")
            details = self.extract_product_details(soup, url)

            if not details.title or not details.current_price:
                logging.warning(f"Incomplete product data for {url}")
                return None

            self.scraped_urls.add(url)
            product_dict = asdict(details)
            await self.add_to_buffer(product_dict)

            logging.info(f"Successfully scraped: {details.title}")
            return product_dict

        except Exception as e:
            logging.error(f"Error scraping {url}: {str(e)}")
            return None

    async def process_search_page(self, session: AsyncSession, page_num: int, search_term: str) -> Tuple[bool, bool]:
        """Process search results page."""
        retry_count = 0
        max_page_retries = 2

        while retry_count < max_page_retries:
            try:
                params = {
                    "_nkw": search_term,
                    "_pgn": page_num,
                    "_ipg": 240,
                }
                url = self.base_search_url + urlencode(params)
                logging.info(f"Processing search page {page_num} (Attempt {retry_count + 1})")

                status_code, html_content = await self._make_request(session, url, "search_page")

                if not html_content:
                    retry_count += 1
                    await asyncio.sleep(self.retry_delay * (retry_count + 1))
                    continue

                soup = BeautifulSoup(html_content, "lxml")
                product_urls = self.extract_product_urls(soup)

                if not product_urls:
                    logging.info(f"No more products found for '{search_term}' after {self.items_scraped} items")
                    return False, True

                logging.info(f"Found {len(product_urls)} products on page {page_num}")

                chunk_size = 5
                for i in range(0, len(product_urls), chunk_size):
                    if self.max_items and self.items_scraped >= self.max_items:
                        logging.info(f"Reached maximum items limit ({self.max_items})")
                        return False, False

                    chunk = product_urls[i: i + chunk_size]
                    remaining_items = self.max_items - self.items_scraped if self.max_items else None

                    if remaining_items:
                        chunk = chunk[:remaining_items]

                    tasks = [self.scrape_product(session, url) for url in chunk]
                    results = await asyncio.gather(*tasks)
                    valid_results = [r for r in results if r is not None]

                    if valid_results:
                        self.items_scraped += len(valid_results)
                        self.products.extend(valid_results)
                        await self.force_save_buffer()
                        logging.info(f"Progress: {self.items_scraped}/{self.max_items if self.max_items else 'unlimited'} items")

                    await asyncio.sleep(1)

                has_next = soup.select_one('a[type="next"]') is not None
                return (has_next and (not self.max_items or self.items_scraped < self.max_items)), False

            except Exception as e:
                logging.error(f"Error processing page {page_num}: {str(e)}")
                retry_count += 1
                await asyncio.sleep(self.retry_delay * (retry_count + 1))

        logging.error(f"Failed to process page {page_num} after all retries")
        return False, False

    async def add_to_buffer(self, product: Dict):
        """Add product to buffer and save if buffer is full."""
        self.product_buffer.append(product)
        if len(self.product_buffer) >= self.batch_size:
            await self.save_batch(list(self.product_buffer))
            self.product_buffer.clear()

    async def force_save_buffer(self):
        """Force save current buffer contents."""
        if self.product_buffer and self.current_output_file:
            await self.save_batch(list(self.product_buffer))
            self.product_buffer.clear()

    async def save_batch(self, batch: List[Dict]):
        """Save batch of products to file."""
        if not batch:
            return

        try:
            self.save_count += 1
            if self.save_count % self.backup_interval == 0:
                backup_file = f"{self.current_output_file}.backup"
                await FileHandler.save_to_file(backup_file, {"products": self.products}, True)

            existing_data = {"products": []}
            if os.path.exists(self.current_output_file):
                async with aiofiles.open(self.current_output_file, "r", encoding="utf-8") as f:
                    content = await f.read()
                    if content:
                        existing_data = json.loads(content)

            new_products = [p for p in batch if p["url"] not in self.saved_product_ids]
            for product in new_products:
                self.saved_product_ids.add(product["url"])

            all_products = existing_data.get("products", []) + new_products
            await FileHandler.save_to_file(self.current_output_file, {"products": all_products})

            self.total_saved = len(all_products)
            logging.info(f"Saved batch of {len(new_products)} new products. Total saved: {self.total_saved}")

        except Exception as e:
            logging.error(f"Error saving batch: {str(e)}")
            await FileHandler.create_emergency_backup(self.current_output_file, {"products": batch})

    async def scrape_all(self, search_terms: List[str], output_dir: str) -> Dict[str, Dict]:
        """Main method to scrape all products for given search terms."""
        all_results = {}

        for search_term in search_terms:
            self.items_scraped = 0
            self.products = []
            self.scraped_urls = set()
            self.product_buffer.clear()

            output_file = Path(output_dir) / f"ebay_products_{self.domain}_{search_term}.json"
            self.current_output_file = str(output_file).replace(" ", "_")

            logging.info(f"Starting scrape for search term: {search_term}")

            async with AsyncSession() as session:
                page_num = 1
                try:
                    while True:
                        has_next, no_more_items = await self.process_search_page(session, page_num, search_term)

                        if no_more_items:
                            logging.warning(f"Search term '{search_term}' has fewer products ({self.items_scraped}) than requested ({self.max_items})")
                            break

                        if not has_next:
                            if self.max_items and self.items_scraped < self.max_items:
                                logging.warning(f"Could only find {self.items_scraped} items for '{search_term}', fewer than requested {self.max_items}")
                            else:
                                logging.info(f"Completed scraping for '{search_term}' with {self.items_scraped} items")
                            break

                        page_num += 1
                        await asyncio.sleep(1)

                    await self.force_save_buffer()
                    all_results[search_term] = {
                        "items_found": self.items_scraped,
                        "items_requested": self.max_items,
                        "products": self.products.copy(),
                    }

                except Exception as e:
                    logging.error(f"Error during scraping '{search_term}': {str(e)}")
                    await self.force_save_buffer()
                    all_results[search_term] = {
                        "items_found": self.items_scraped,
                        "items_requested": self.max_items,
                        "products": self.products.copy(),
                        "error": str(e),
                    }

        return all_results


async def main():
    """Main entry point of the scraper."""
    try:
        os.makedirs("output", exist_ok=True)

        domain = "GB"
        print(f"Scraping eBay domain: 🌐 {domain} - {EBAY_DOMAIN[domain]}")

        search_terms = [term.strip() for term in input("\nEnter search terms (comma-separated): ").strip().split(",")]
        max_items_input = input("\nEnter maximum items to scrape (leave empty for all): ").strip()
        max_items = int(max_items_input) if max_items_input else None

        scraper = EbayRealTimeScraper(
            domain=domain,
            max_retries=3,
            max_concurrent_requests=3,
            batch_size=50,
            max_items=max_items
        )

        results = await scraper.scrape_all(search_terms=search_terms, output_dir="output")

        for term, data in results.items():
            logging.info(f"Scraped {len(data['products'])} products for '{term}'")

    except KeyboardInterrupt:
        logging.info("Scraping interrupted by user")
        if "scraper" in locals():
            await scraper.force_save_buffer()
    except Exception as e:
        logging.error(f"Fatal error in main: {str(e)}")
        if "scraper" in locals():
            await scraper.force_save_buffer()
    finally:
        logging.info("Scraping completed. Check output directory for results.")


if __name__ == "__main__":
    if platform.system() == "Windows":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Process terminated by user")
    except Exception as e:
        logging.error(f"Fatal error: {str(e)}")
