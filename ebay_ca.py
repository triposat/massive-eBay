import asyncio
import json
import logging
import os
import platform
import re
import signal
import sys
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Deque

import aiofiles
from bs4 import BeautifulSoup
from curl_cffi.requests import AsyncSession
from dotenv import load_dotenv
from urllib.parse import urlencode

# Load environment variables
load_dotenv()

# Configure stdout/stderr encoding
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

# Constants
EBAY_DOMAINS = {
    "CA": "https://www.ebay.ca"
}

# Configure logging


class UTF8FileHandler(logging.FileHandler):
    """Custom file handler that ensures UTF-8 encoding"""

    def __init__(self, filename: str, mode: str = "a", encoding: str = "utf-8"):
        super().__init__(filename, mode, encoding)

    def emit(self, record):
        try:
            msg = self.format(record)
            stream = self.stream
            stream.write(msg + self.terminator)
            self.flush()
        except Exception:
            self.handleError(record)


def setup_logging():
    """Configure logging settings"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            UTF8FileHandler("ebay_scraper.log"),
            logging.StreamHandler(sys.stdout)
        ],
    )
    return logging.getLogger(__name__)


logger = setup_logging()


@dataclass
class ProductDetails:
    """Data class to store product information"""
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


class EbayRealTimeScraper:
    """Main scraper class with modular functionality"""

    def __init__(
        self,
        domain: str,
        max_retries: int = 3,
        max_concurrent_requests: int = 3,
        batch_size: int = 50,
        max_items: Optional[int] = None,
    ):
        self._initialize_proxy_settings(domain)
        self._initialize_scraper_settings(
            max_retries, max_concurrent_requests, batch_size, max_items
        )
        self._initialize_state_tracking()
        self.setup_signal_handlers()

    def _initialize_proxy_settings(self, domain: str):
        """Initialize proxy-related settings"""
        self.proxy_host = "network.joinmassive.com:65534"
        self.username = os.getenv("PROXY_USERNAME")
        self.password = os.getenv("PROXY_PASSWORD")
        self.domain = domain

        if not self.username or not self.password:
            raise ValueError("Proxy credentials not found in environment variables")
        self.proxy_auth = f"{
            self.username}-country-{self.domain}:{self.password}"
        self.base_search_url = EBAY_DOMAINS.get(self.domain)

        if not self.base_search_url:
            raise ValueError(
                f"Invalid domain identifier. Valid options are: {
                             ', '.join(EBAY_DOMAINS.keys())}"
            )
        self.base_search_url += "/sch/i.html?"

    def _initialize_scraper_settings(
        self,
        max_retries: int,
        max_concurrent_requests: int,
        batch_size: int,
        max_items: Optional[int],
    ):
        """Initialize scraper-specific settings"""
        self.max_concurrent_requests = max_concurrent_requests
        self.max_retries = max_retries
        self.batch_size = batch_size
        self.max_items = max_items
        self.page_load_timeout = 30
        self.retry_delay = 5
        self.retry_statuses = {408, 429, 500, 502, 503, 504}
        self.min_content_length = 50000
        self.required_elements = {
            "search_page": ["ul.srp-results", "li.s-item"],
            "product_page": ["div.x-item-title", "div.x-price-section"],
        }

    def _initialize_state_tracking(self):
        """Initialize state tracking variables"""
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

    def _get_proxy_config(self) -> Dict[str, str]:
        """Get proxy configuration"""
        return {"https": f"http://{self.proxy_auth}@{self.proxy_host}"}

    def setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown"""
        try:
            for sig in (signal.SIGTERM, signal.SIGINT):
                signal.signal(sig, self.handle_shutdown)
        except AttributeError:
            pass

    def handle_shutdown(self, signum, frame):
        """Handle shutdown signals"""
        logger.info("Shutdown signal received. Saving remaining data...")
        if self.product_buffer and self.current_output_file:
            loop = asyncio.get_event_loop()
            loop.run_until_complete(self.force_save_buffer())
        logger.info("Shutdown complete")
        sys.exit(0)

    def validate_page_content(
        self, html_content: str, page_type: str
    ) -> Tuple[bool, str]:
        """Validate the content of a page"""
        if not html_content or len(html_content) < self.min_content_length:
            return False, "Content too short"
        soup = BeautifulSoup(html_content, "lxml")
        error_texts = [
            "Sorry, the page you requested was not found",
            "This listing was ended",
            "Security Measure",
            "Robot Check",
        ]

        for error in error_texts:
            if error.lower() in html_content.lower():
                return False, f"Error page detected: {error}"
        for selector in self.required_elements[page_type]:
            if not soup.select(selector):
                return False, f"Missing required element: {selector}"
        return True, "Page content valid"

    async def _make_request(
        self, session: AsyncSession, url: str, page_type: str
    ) -> Tuple[int, Optional[str]]:
        """Make an HTTP request with retry logic"""
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
                        is_valid, message = self.validate_page_content(
                            content, page_type
                        )

                        if is_valid:
                            return response.status_code, content
                        else:
                            logger.warning(
                                f"Attempt {retries + 1}: Invalid content: {message}"
                            )
                            if (
                                "Robot Check" in message
                                or "Security Measure" in message
                            ):
                                await asyncio.sleep(self.retry_delay * (retries + 1))
                                retries += 1
                                continue
                    elif response.status_code in self.retry_statuses:
                        logger.warning(
                            f"Attempt {retries + 1}: Got status {response.status_code} for {url}"
                        )
                        await asyncio.sleep(self.retry_delay * (retries + 1))
                        retries += 1
                        continue
                    else:
                        return response.status_code, None
            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {retries + 1} failed for {url}: {last_error}")

                if (
                    "timeout" in last_error.lower()
                    or "connection" in last_error.lower()
                ):
                    await asyncio.sleep(self.retry_delay * (retries + 1))
                    retries += 1
                    continue
                else:
                    return None, None
            retries += 1
            await asyncio.sleep(self.retry_delay * retries)
        logger.error(
            f"All retry attempts failed for {
                     url}. Last error: {last_error}"
        )
        return None, None

    @staticmethod
    def _extract_store_info(soup: BeautifulSoup) -> Dict[str, str]:
        """Extract seller/store information from the page"""
        store_info = {"name": "", "feedback": "", "sales": ""}

        try:
            if store_header := soup.select_one("div.x-store-information__header"):
                if store_name := store_header.select_one("span.ux-textspans--BOLD"):
                    store_info["name"] = store_name.text.strip()
                if highlights := store_header.select_one(
                    "h4.x-store-information__highlights"
                ):
                    for span in highlights.select("span.ux-textspans"):
                        store_info["feedback"] = span.text.strip()
                        break
                    sales_spans = highlights.select("span.ux-textspans--SECONDARY")
                    if len(sales_spans) >= 2:
                        store_info["sales"] = (
                            f"{sales_spans[0].text.strip()} {
                            sales_spans[1].text.strip()}"
                        )
        except Exception as e:
            logging.error(f"Error extracting store information: {str(e)}")
        return store_info

    def extract_product_urls(self, soup: BeautifulSoup) -> List[str]:
        """Extract product URLs from search results page"""
        urls = []
        links = soup.select("a.s-item__link")
        for link in links:
            url = link.get("href", "")
            if url and "itm/" in url:
                try:
                    item_id = url.split("itm/")[1].split("?")[0]
                    clean_url = f"{self.base_search_url.split(
                        '/sch')[0]}/itm/{item_id}"
                    if clean_url not in self.scraped_urls:
                        urls.append(clean_url)
                except IndexError:
                    continue
        return urls

    def extract_product_details(self, soup: BeautifulSoup, url: str) -> ProductDetails:
        """Extract product details from product page"""
        details = ProductDetails(url=url)

        try:
            details.store_info = self._extract_store_info(soup)

            # Extract title and subtitle

            if title_div := soup.select_one("div.x-item-title"):
                if title := title_div.select_one("h1.x-item-title__mainTitle span"):
                    details.title = title.text.strip()
                if subtitle := title_div.select_one("div.x-item-title__subTitle span"):
                    details.subtitle = subtitle.text.strip()
            # Extract price information

            if price_section := soup.select_one("div.x-price-section"):
                if current_price := price_section.select_one(
                    "div.x-price-primary span"
                ):
                    details.current_price = current_price.text.strip()
                if was_price := price_section.select_one(
                    "span.ux-textspans--STRIKETHROUGH"
                ):
                    details.was_price = was_price.text.strip()
                # Extract discount

                discount = None
                if emphasis_discount := price_section.select_one(
                    "span.ux-textspans--EMPHASIS"
                ):
                    discount = emphasis_discount.text.strip()
                elif secondary_discount := price_section.select_one(
                    "span.ux-textspans--SECONDARY"
                ):
                    discount = secondary_discount.text.strip()
                if discount and (percentage_match := re.search(r"(\d+)%", discount)):
                    details.discount = percentage_match.group(1) + "%"
            # Extract availability and sold count

            if quantity_div := soup.select_one("div.x-quantity__availability"):
                spans = quantity_div.select("span.ux-textspans")
                if len(spans) >= 1:
                    details.availability = spans[0].text.strip()
                if len(spans) >= 2:
                    details.sold_count = spans[1].text.strip()
            # Extract shipping and location information

            self._extract_shipping_info(soup, details)

            # Extract returns information

            self._extract_returns_info(soup, details)

            # Extract condition, brand, and type

            if condition_span := soup.select_one(
                "div.x-item-condition-max-view .ux-section__item > span.ux-textspans"
            ):
                details.condition = condition_span.text.strip().split(".")[0] + "."
            self._extract_brand_and_type(soup, details)
        except Exception as e:
            logger.error(f"Error extracting details from {url}: {str(e)}")
        return details

    def _extract_shipping_info(self, soup: BeautifulSoup, details: ProductDetails):
        """Extract shipping and location information"""
        if shipping_div := soup.select_one("div.d-shipping-minview"):
            if shipping_row := shipping_div.select_one(
                "div.ux-layout-section__row div.ux-labels-values-with-hints[data-testid='ux-labels-values-with-hints'] "
                "div.ux-labels-values--shipping"
            ):
                shipping_text_parts = []

                if shipping_section := shipping_row.select_one(
                    "div.ux-labels-values__values-content"
                ):
                    for span in shipping_section.find_all(
                        "span", class_="ux-textspans"
                    ):
                        if span.text.strip() and "See details" not in span.text:
                            text = span.text.strip()
                            if text and text not in [".", "&nbsp;", " "]:
                                shipping_text_parts.append(text)
                    details.shipping = " ".join(shipping_text_parts).strip()
                    if "Located in:" in details.shipping:
                        details.shipping = details.shipping.split("Located in:")[
                            0
                        ].strip()
                    if details.shipping.endswith("."):
                        details.shipping = details.shipping[:-1]
                if location_span := shipping_row.select_one(
                    "div.ux-labels-values__values-content span.ux-textspans--SECONDARY"
                ):
                    location_text = location_span.text.strip()
                    if "Located in:" in location_text:
                        details.location = location_text.split("Located in:")[
                            -1
                        ].strip()

    def _extract_returns_info(self, soup: BeautifulSoup, details: ProductDetails):
        """Extract returns information"""
        if returns_div := soup.select_one("div.x-returns-minview"):
            if returns_section := returns_div.select_one(
                "div.ux-labels-values__values-content"
            ):
                returns_text_parts = []
                spans = returns_section.find_all("span", class_="ux-textspans")

                for span in spans:
                    if (
                        span.text.strip()
                        and "clipped" not in span.get("class", [])
                        and "See details" not in span.text
                    ):

                        text = span.text.strip()
                        if text and text not in [".", "&nbsp;", " "]:
                            returns_text_parts.append(text)
                returns_text = " ".join(returns_text_parts).strip()
                returns_text = " ".join(returns_text.split())
                returns_text = returns_text.replace(" .", ".")
                details.returns = returns_text

    def _extract_brand_and_type(self, soup: BeautifulSoup, details: ProductDetails):
        """Extract brand and type information"""
        # Extract brand

        if (brand_dl := soup.select_one("dl.ux-labels-values--brand")) and (
            brand_value := brand_dl.select_one("dd .ux-textspans")
        ):
            details.brand = brand_value.text.strip()
        elif (brand_div := soup.select_one("div.ux-labels-values--brand")) and (
            brand_value := brand_div.select_one(
                ".ux-labels-values__values .ux-textspans"
            )
        ):
            details.brand = brand_value.text.strip()
        # Extract type

        if (type_dl := soup.select_one("dl.ux-labels-values--type")) and (
            type_value := type_dl.select_one("dd .ux-textspans")
        ):
            details.type = type_value.text.strip()
        elif (type_div := soup.select_one("div.ux-labels-values--type")) and (
            type_value := type_div.select_one(".ux-labels-values__values .ux-textspans")
        ):
            details.type = type_value.text.strip()

    def has_next_page(self, soup: BeautifulSoup) -> bool:
        """Check if there is a next page of search results"""
        next_link = soup.select_one('a[type="next"]')
        if next_link and next_link.get("href"):
            return True
        next_button = soup.select_one('button[type="next"]')
        if next_button and next_button.get("aria-disabled") == "true":
            return False
        return bool(next_link)


    async def save_batch(self, batch: List[Dict]):
        """Save a batch of scraped products to file"""
        if not batch:
            return
        try:
            os.makedirs(os.path.dirname(self.current_output_file), exist_ok=True)

            self.save_count += 1
            if self.save_count % self.backup_interval == 0:
                await self.create_backup()
            temp_file = f"{self.current_output_file}.temp"
            existing_data = await self._load_existing_data()

            new_products = [
                product for product in batch if product["url"] not in self.saved_product_ids
            ]

            for product in new_products:
                self.saved_product_ids.add(product["url"])
            all_products = existing_data.get("products", []) + new_products
            updated_data = {"products": all_products}

            await self._save_to_file(temp_file, updated_data)
            os.replace(temp_file, self.current_output_file)

            self.total_saved = len(all_products)
            logger.info(
                f"Saved batch of {len(new_products)} new products. Total saved: {
                        self.total_saved}"
            )
        except Exception as e:
            logger.error(f"Error saving batch: {str(e)}")
            await self._create_emergency_backup(batch)


    async def _load_existing_data(self) -> Dict:
        """Load existing data from file"""
        existing_data = {
            "products": [],
            "total_products": 0,
            "requested_items": self.max_items,
            "actual_items_found": 0,
        }

        if os.path.exists(self.current_output_file):
            try:
                async with aiofiles.open(
                    self.current_output_file, "r", encoding="utf-8"
                ) as f:
                    content = await f.read()
                    if content:
                        existing_data = json.loads(content)
                        for product in existing_data.get("products", []):
                            self.saved_product_ids.add(product["url"])
            except json.JSONDecodeError:
                logger.error("Error reading existing file, starting fresh")
        return existing_data


    async def _save_to_file(self, filename: str, data: Dict):
        """Save data to file"""
        async with aiofiles.open(filename, "w", encoding="utf-8") as f:
            await f.write(json.dumps(data, indent=2, ensure_ascii=False))


    async def _create_emergency_backup(self, batch: List[Dict]):
        """Create emergency backup in case of save failure"""
        try:
            emergency_backup = f"{self.current_output_file}.emergency_{
                    datetime.now().strftime('%Y%m%d_%H%M%S')}"
            backup_data = {
                "products": batch,
                "total_products": len(batch),
                "requested_items": self.max_items,
                "actual_items_found": len(batch),
                "last_updated": datetime.now().isoformat(),
            }
            await self._save_to_file(emergency_backup, backup_data)
            logger.info(f"Created emergency backup: {emergency_backup}")
        except Exception as backup_error:
            logger.error(f"Emergency backup failed: {str(backup_error)}")


    async def create_backup(self):
        """Create a backup of the current data file"""
        if os.path.exists(self.current_output_file):
            try:
                async with aiofiles.open(
                    self.current_output_file, "r", encoding="utf-8"
                ) as f:
                    current_content = await f.read()
                    current_data = json.loads(current_content)

                    if current_data.get("products") == self.last_saved_products:
                        logger.info("No significant changes detected, skipping backup")
                        return
                backup_file = f"{self.current_output_file}.backup"
                await self._save_to_file(backup_file, current_data)
                logger.info(f"Backup updated: {backup_file}")
                self.last_saved_products = current_data.get("products")
            except Exception as e:
                logger.error(f"Failed to create backup: {str(e)}")


    async def force_save_buffer(self):
        """Force save the current buffer contents"""
        if self.product_buffer and self.current_output_file:
            await self.save_batch(list(self.product_buffer))
            self.product_buffer.clear()


    async def add_to_buffer(self, product: Dict):
        """Add a product to the buffer and save if full"""
        self.product_buffer.append(product)
        if len(self.product_buffer) >= self.batch_size:
            await self.save_batch(list(self.product_buffer))
            self.product_buffer.clear()


    async def scrape_product(self, session: AsyncSession, url: str) -> Optional[Dict]:
        """Scrape a single product page"""
        try:
            status_code, html_content = await self._make_request(
                session, url, "product_page"
            )

            if not html_content:
                logger.error(f"Failed to load product page properly: {url}")
                return None
            soup = BeautifulSoup(html_content, "lxml")
            details = self.extract_product_details(soup, url)

            if not details.title or not details.current_price:
                logger.warning(f"Incomplete product data for {url}")
                return None
            self.scraped_urls.add(url)
            product_dict = asdict(details)
            await self.add_to_buffer(product_dict)

            logger.info(f"Successfully scraped: {details.title}")
            return product_dict
        except Exception as e:
            logger.error(f"Error scraping {url}: {str(e)}")
            return None


    async def process_search_page(
        self, session: AsyncSession, page_num: int, search_term: str
    ) -> Tuple[bool, bool]:
        """Process a single search results page"""
        retry_count = 0
        max_page_retries = 2
        no_more_items = False

        while retry_count < max_page_retries:
            try:
                params = {
                    "_nkw": search_term,
                    "_pgn": page_num,
                    "_ipg": 240,
                }
                url = self.base_search_url + urlencode(params)
                logger.info(
                    f"Processing search page {
                            page_num} (Attempt {retry_count + 1})"
                )

                status_code, html_content = await self._make_request(
                    session, url, "search_page"
                )

                if not html_content:
                    retry_count += 1
                    await asyncio.sleep(self.retry_delay * (retry_count + 1))
                    continue
                soup = BeautifulSoup(html_content, "lxml")
                product_urls = self.extract_product_urls(soup)

                if not product_urls:
                    logger.info(
                        f"No more products found for '{
                                search_term}' after {self.items_scraped} items"
                    )
                    no_more_items = True
                    return False, True
                logger.info(
                    f"Found {len(product_urls)
                                    } products on page {page_num}"
                )

                await self._process_product_urls(session, product_urls)

                has_next = self.has_next_page(soup)
                return (
                    has_next
                    and (not self.max_items or self.items_scraped < self.max_items),
                    False,
                )
            except Exception as e:
                logger.error(f"Error processing page {page_num}: {str(e)}")
                retry_count += 1
                await asyncio.sleep(self.retry_delay * (retry_count + 1))
        logger.error(f"Failed to process page {page_num} after all retries")
        return False, False


    async def _process_product_urls(self, session: AsyncSession, product_urls: List[str]):
        """Process a batch of product URLs"""
        chunk_size = 5
        for i in range(0, len(product_urls), chunk_size):
            if self.max_items and self.items_scraped >= self.max_items:
                logger.info(f"Reached maximum items limit ({self.max_items})")
                return
            chunk = product_urls[i : i + chunk_size]
            remaining_items = (
                self.max_items - self.items_scraped if self.max_items else None
            )

            if remaining_items:
                chunk = chunk[:remaining_items]
            tasks = [self.scrape_product(session, url) for url in chunk]
            results = await asyncio.gather(*tasks)
            valid_results = [r for r in results if r is not None]

            if valid_results:
                self.items_scraped += len(valid_results)
                self.products.extend(valid_results)
                await self.force_save_buffer()

                logger.info(
                    f"Progress: {
                            self.items_scraped}/{self.max_items if self.max_items else 'unlimited'} items"
                )
            await asyncio.sleep(1)


    async def scrape_all(self, search_terms: List[str], output_dir: str) -> Dict[str, Dict]:
        """Main method to scrape multiple search terms"""
        all_results = {}

        for search_term in search_terms:
            self._reset_scraper_state()

            output_file = (
                Path(output_dir) / f"ebay_products_{self.domain}_{search_term}.json"
            )
            self.current_output_file = str(output_file).replace(" ", "_")

            logger.info(f"Starting scrape for search term: {search_term}")

            async with AsyncSession() as session:
                page_num = 1
                try:
                    while True:
                        has_next, no_more_items = await self.process_search_page(
                            session, page_num, search_term
                        )

                        if no_more_items:
                            logger.warning(
                                f"Search term '{search_term}' has fewer products ({
                                    self.items_scraped}) "
                                f"than requested ({self.max_items})"
                            )
                            break
                        if not has_next:
                            if self.max_items and self.items_scraped < self.max_items:
                                logger.warning(
                                    f"Could only find {self.items_scraped} items for '{
                                        search_term}', "
                                    f"fewer than requested {self.max_items}"
                                )
                            else:
                                logger.info(
                                    f"Completed scraping for '{
                                            search_term}' with {self.items_scraped} items"
                                )
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
                    logger.error(
                        f"Error during scraping '{
                                search_term}': {str(e)}"
                    )
                    await self.force_save_buffer()
                    all_results[search_term] = {
                        "items_found": self.items_scraped,
                        "items_requested": self.max_items,
                        "products": self.products.copy(),
                        "error": str(e),
                    }
        return all_results


    def _reset_scraper_state(self):
        """Reset scraper state for a new search term"""
        self.items_scraped = 0
        self.products = []
        self.scraped_urls = set()
        self.product_buffer.clear()

async def main():
    """Main entry point of the scraper"""
    try:
        os.makedirs("output", exist_ok=True)

        domain = "CA"
        print(f"Scraping eBay domain: 🌐 {domain} - {EBAY_DOMAINS[domain]}")

        search_terms = [
            term.strip()
            for term in input("\nEnter search terms (comma-separated): ")
            .strip()
            .split(",")
        ]
        max_items_input = input(
            "\nEnter maximum items to scrape (leave empty for all): "
        ).strip()
        max_items = int(max_items_input) if max_items_input else None

        scraper = EbayRealTimeScraper(
            domain=domain,
            max_retries=3,
            max_concurrent_requests=3,
            batch_size=50,
            max_items=max_items,
        )

        results = await scraper.scrape_all(
            search_terms=search_terms, output_dir="output"
        )

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
