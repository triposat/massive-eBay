from curl_cffi.requests import AsyncSession
from bs4 import BeautifulSoup
import json
import asyncio
from datetime import datetime
from urllib.parse import urlencode
from typing import List, Dict, Set, Optional, Tuple, Deque
import platform
import logging
import aiofiles
from dataclasses import dataclass, asdict, field
from dotenv import load_dotenv
import os
from pathlib import Path
import signal
import sys
from collections import deque
import re

# Load environment variables
load_dotenv()


# Configure system settings
def configure_system():
    """Configure system-specific settings."""
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")


# Constants
class ScraperConfig:
    """Configuration constants for the scraper."""
    EBAY_DOMAINS = {"NL": "https://www.ebay.nl"}

    REQUIRED_ELEMENTS = {
        "search_page": ["ul.srp-results", "li.s-item"],
        "product_page": ["div.x-item-title", "div.x-price-section"]
    }

    ERROR_TEXTS = [
        "Sorry, the page you requested was not found",
        "This listing was ended",
        "Security Measure",
        "Robot Check"
    ]

    RETRY_STATUSES = {408, 429, 500, 502, 503, 504}
    PAGE_LOAD_TIMEOUT = 30
    RETRY_DELAY = 5
    MIN_CONTENT_LENGTH = 50000


@dataclass
class ProductDetails:
    """Data class to store product information."""
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


class LoggingManager:
    """Manages logging configuration and operations."""

    class UTF8FileHandler(logging.FileHandler):
        """Custom file handler ensuring UTF-8 encoding."""

        def __init__(self, filename, mode="a", encoding="utf-8"):
            super().__init__(filename, mode, encoding)

        def emit(self, record):
            try:
                msg = self.format(record)
                stream = self.stream
                stream.write(msg + self.terminator)
                self.flush()
            except Exception:
                self.handleError(record)

    @staticmethod
    def setup_logging():
        """Configure logging with both file and console handlers."""
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[
                LoggingManager.UTF8FileHandler("ebay_scraper.log"),
                logging.StreamHandler(sys.stdout)
            ]
        )
        return logging.getLogger(__name__)


class ProxyManager:
    """Manages proxy configuration and authentication."""

    def __init__(self, domain: str):
        self.proxy_host = "network.joinmassive.com:65534"
        self.username = os.getenv("PROXY_USERNAME")
        self.password = os.getenv("PROXY_PASSWORD")
        self.domain = domain
        self._validate_credentials()
        self.proxy_auth = f"{self.username}-country-{self.domain}:{self.password}"
        self.base_url = ScraperConfig.EBAY_DOMAINS.get(domain)

        if not self.base_url:
            raise ValueError(
                f"Invalid domain identifier. Valid options are: {', '.join(ScraperConfig.EBAY_DOMAINS.keys())}")

    def _validate_credentials(self):
        """Validate proxy credentials."""
        if not self.username or not self.password:
            raise ValueError("Proxy credentials not found in environment variables")

    def get_proxy_config(self) -> Dict[str, str]:
        """Get proxy configuration dictionary."""
        return {"https": f"http://{self.proxy_auth}@{self.proxy_host}"}


class DataManager:
    """Manages data operations including saving and loading."""

    def __init__(self, output_file: str, max_items: Optional[int] = None):
        self.output_file = output_file
        self.saved_product_ids = set()
        self.max_items = max_items
        self.save_count = 0
        self.backup_interval = 20

    async def load_existing_data(self) -> Dict:
        """Load existing data from the output file."""
        existing_data = {
            "products": [],
            "total_products": 0,
            "requested_items": self.max_items,
            "actual_items_found": 0
        }

        if os.path.exists(self.output_file):
            try:
                async with aiofiles.open(self.output_file, "r", encoding="utf-8") as f:
                    content = await f.read()
                    if content:
                        existing_data = json.loads(content)
                        for product in existing_data.get("products", []):
                            self.saved_product_ids.add(product["url"])
            except json.JSONDecodeError:
                logging.error("Error reading existing file, starting fresh")

        return existing_data

    async def save_batch(self, batch: List[Dict], existing_data: Optional[Dict] = None):
        """Save a batch of products to file."""
        if not batch:
            return

        try:
            os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
            self.save_count += 1

            if self.save_count % self.backup_interval == 0:
                await self.create_backup()

            if existing_data is None:
                existing_data = await self.load_existing_data()

            new_products = [
                product for product in batch
                if product["url"] not in self.saved_product_ids
            ]

            for product in new_products:
                self.saved_product_ids.add(product["url"])

            updated_data = {
                "products": existing_data.get("products", []) + new_products
            }

            await self.save_data(updated_data)
            logging.info(f"Saved batch of {len(new_products)} new products")

        except Exception as e:
            logging.error(f"Error saving batch: {str(e)}")
            await self.create_emergency_backup(batch)

    async def save_data(self, data: Dict):
        """Save data to file."""
        temp_file = f"{self.output_file}.temp"
        try:
            async with aiofiles.open(temp_file, "w", encoding="utf-8") as f:
                await f.write(json.dumps(data, indent=2, ensure_ascii=False))
            os.replace(temp_file, self.output_file)
        except Exception as e:
            logging.error(f"Error saving data: {str(e)}")
            if os.path.exists(temp_file):
                os.remove(temp_file)

    async def create_backup(self):
        """Create a backup of the current data file."""
        if not os.path.exists(self.output_file):
            return

        try:
            backup_file = f"{self.output_file}.backup"
            async with aiofiles.open(self.output_file, "r", encoding="utf-8") as source:
                content = await source.read()
            async with aiofiles.open(backup_file, "w", encoding="utf-8") as target:
                await target.write(content)
            logging.info(f"Created backup: {backup_file}")
        except Exception as e:
            logging.error(f"Failed to create backup: {str(e)}")

    async def create_emergency_backup(self, batch: List[Dict]):
        """Create an emergency backup in case of save failure."""
        try:
            emergency_file = f"{self.output_file}.emergency_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            emergency_data = {
                "products": batch,
                "total_products": len(batch),
                "requested_items": self.max_items,
                "timestamp": datetime.now().isoformat()
            }

            async with aiofiles.open(emergency_file, "w", encoding="utf-8") as f:
                await f.write(json.dumps(emergency_data, indent=2, ensure_ascii=False))
            logging.info(f"Created emergency backup: {emergency_file}")
        except Exception as e:
            logging.error(f"Emergency backup failed: {str(e)}")


class ContentValidator:
    """Validates scraped content."""
    @staticmethod
    def validate_page_content(html_content: str, page_type: str) -> Tuple[bool, str]:
        """Validate the content of a scraped page."""
        if not html_content or len(html_content) < ScraperConfig.MIN_CONTENT_LENGTH:
            return False, "Content too short"

        soup = BeautifulSoup(html_content, "lxml")

        for error in ScraperConfig.ERROR_TEXTS:
            if error.lower() in html_content.lower():
                return False, f"Error page detected: {error}"

        for selector in ScraperConfig.REQUIRED_ELEMENTS[page_type]:
            if not soup.select(selector):
                return False, f"Missing required element: {selector}"

        return True, "Page content valid"


class ContentExtractor:
    """Extracts and processes content from HTML."""
    @staticmethod
    def extract_store_info(soup: BeautifulSoup, details: ProductDetails):
        """Extract seller/store information."""
        try:
            if store_wrapper := soup.select_one("div.x-store-information__header"):
                if store_name := store_wrapper.select_one("span.ux-textspans--BOLD"):
                    details.store_info["name"] = store_name.text.strip()

                if highlights := store_wrapper.select_one("h4.x-store-information__highlights"):
                    for span in highlights.select("span.ux-textspans"):
                        details.store_info["feedback"] = span.text.strip()
                        break

                    sales_spans = highlights.select("span.ux-textspans--SECONDARY")
                    if len(sales_spans) >= 2:
                        count = sales_spans[0].text.strip()
                        text = sales_spans[1].text.strip()
                        details.store_info["sales"] = f"{count} {text}"
        except Exception as e:
            logging.error(f"Error extracting store info: {str(e)}")

    @staticmethod
    def extract_navigation_info(soup: BeautifulSoup, base_url: str) -> Tuple[bool, List[str]]:
        """Extract navigation information and product URLs."""
        has_next = bool(soup.select_one('a[type="next"]'))
        if not has_next:
            next_button = soup.select_one('button[type="next"]')
            has_next = next_button and next_button.get("aria-disabled") != "true"

        urls = []
        for link in soup.select("a.s-item__link"):
            url = link.get("href", "")
            if url and "itm/" in url:
                try:
                    item_id = url.split("itm/")[1].split("?")[0]
                    clean_url = f"{base_url}/itm/{item_id}"
                    urls.append(clean_url)
                except IndexError:
                    continue

        return has_next, urls

    @staticmethod
    def extract_product_details(soup: BeautifulSoup, url: str) -> ProductDetails:
        """Extract all product details from product page."""
        details = ProductDetails(url=url)

        try:
            ContentExtractor.extract_store_info(soup, details)
            ContentExtractor._extract_basic_info(soup, details)
            ContentExtractor._extract_pricing_info(soup, details)
            ContentExtractor._extract_availability_info(soup, details)
            ContentExtractor._extract_shipping_info(soup, details)
            ContentExtractor._extract_condition_info(soup, details)
            ContentExtractor._extract_additional_info(soup, details)
        except Exception as e:
            logging.error(f"Error extracting details from {url}: {str(e)}")

        return details

    @staticmethod
    def _extract_basic_info(soup: BeautifulSoup, details: ProductDetails):
        """Extract basic product information."""
        if title_div := soup.select_one("div.x-item-title"):
            if title := title_div.select_one("h1.x-item-title__mainTitle span"):
                details.title = title.text.strip()
            if subtitle := title_div.select_one("div.x-item-title__subTitle span"):
                details.subtitle = subtitle.text.strip()

    @staticmethod
    def _extract_pricing_info(soup: BeautifulSoup, details: ProductDetails):
        """Extract price-related information."""
        if price_section := soup.select_one("div.x-price-section"):
            if current_price := price_section.select_one("div.x-price-primary span"):
                details.current_price = current_price.text.strip()
            if was_price := price_section.select_one("span.ux-textspans--STRIKETHROUGH"):
                details.was_price = was_price.text.strip()

            # Extract discount percentage
            if emphasis_discount := price_section.select_one("span.ux-textspans--EMPHASIS"):
                if percentage_match := re.search(r'(\d+)%', emphasis_discount.text):
                    details.discount = percentage_match.group(1) + '%'

    @staticmethod
    def _extract_availability_info(soup: BeautifulSoup, details: ProductDetails):
        """Extract availability information."""
        if quantity_div := soup.select_one("div.x-quantity__availability"):
            spans = quantity_div.select("span.ux-textspans")
            if spans:
                details.availability = spans[0].text.strip()
            if len(spans) > 1:
                details.sold_count = spans[1].text.strip()

    @staticmethod
    def _extract_shipping_info(soup: BeautifulSoup, details: ProductDetails):
        """Extract shipping and location information."""
        if shipping_div := soup.select_one("div.d-shipping-minview"):
            if shipping_section := shipping_div.select_one("div.ux-labels-values__values-content"):
                shipping_text_parts = []
                for span in shipping_section.select("span.ux-textspans"):
                    text = span.text.strip()
                    if text and text not in ['.', 'Details', 'See details']:
                        shipping_text_parts.append(text)
                details.shipping = " ".join(shipping_text_parts).strip()

                # Extract location
                for div in shipping_section.find_all("div"):
                    if loc_span := div.find("span", class_="ux-textspans--SECONDARY"):
                        location_text = loc_span.text.strip()
                        if ":" in location_text:
                            location_text = location_text.split(": ", 1)[-1]
                        details.location = location_text
                        break

    @staticmethod
    def _extract_condition_info(soup: BeautifulSoup, details: ProductDetails):
        """Extract condition information."""
        if condition_span := soup.select_one("div.x-item-condition-max-view .ux-section__item > span.ux-textspans"):
            details.condition = condition_span.text.strip()

    @staticmethod
    def _extract_additional_info(soup: BeautifulSoup, details: ProductDetails):
        """Extract additional product information like brand and type."""
        # Extract brand
        if brand_div := soup.select_one("div.ux-labels-values--brand"):
            if brand_value := brand_div.select_one(".ux-labels-values__values .ux-textspans"):
                details.brand = brand_value.text.strip()

        # Extract type
        if type_div := soup.select_one("div.ux-labels-values--type"):
            if type_value := type_div.select_one(".ux-labels-values__values .ux-textspans"):
                details.type = type_value.text.strip()


class EbayRealTimeScraper:
    """Main scraper class handling the scraping process."""

    def __init__(
        self,
        domain: str,
        max_retries: int = 3,
        max_concurrent_requests: int = 3,
        batch_size: int = 50,
        max_items: Optional[int] = None
    ):
        self.logger = LoggingManager.setup_logging()
        self.proxy_manager = ProxyManager(domain)
        self.max_retries = max_retries
        self.max_concurrent_requests = max_concurrent_requests
        self.batch_size = batch_size
        self.max_items = max_items
        self._initialize_state()
        self.setup_signal_handlers()

    def _initialize_state(self):
        """Initialize scraper state variables."""
        self.scraped_urls = set()
        self.products = []
        self.product_buffer = deque(maxlen=self.batch_size)
        self.items_scraped = 0
        self.semaphore = asyncio.Semaphore(self.max_concurrent_requests)

    def setup_signal_handlers(self):
        """Set up handlers for system signals."""
        try:
            for sig in (signal.SIGTERM, signal.SIGINT):
                signal.signal(sig, self.handle_shutdown)
        except AttributeError:
            pass

    def handle_shutdown(self, signum, frame):
        """Handle system shutdown signals."""
        self.logger.info("Shutdown signal received. Saving remaining data...")
        if self.products:
            loop = asyncio.get_event_loop()
            loop.run_until_complete(self.force_save_buffer())
        self.logger.info("Shutdown complete")
        sys.exit(0)

    async def force_save_buffer(self):
        """Force save any remaining products in the buffer."""
        if self.products and hasattr(self, 'data_manager'):
            await self.data_manager.save_batch(self.products)
            self.products = []

    async def _make_request(self, session: AsyncSession, url: str, page_type: str) -> Tuple[Optional[int], Optional[str]]:
        """Make an HTTP request with retry logic."""
        for attempt in range(self.max_retries):
            try:
                async with self.semaphore:
                    response = await session.get(
                        url,
                        proxies=self.proxy_manager.get_proxy_config(),
                        impersonate="chrome124",
                        timeout=ScraperConfig.PAGE_LOAD_TIMEOUT,
                        verify=False
                    )

                    if response.status_code == 200:
                        content = response.text
                        is_valid, message = ContentValidator.validate_page_content(
                            content, page_type)

                        if is_valid:
                            return response.status_code, content
                        elif "Robot Check" in message or "Security Measure" in message:
                            await asyncio.sleep(ScraperConfig.RETRY_DELAY * (attempt + 1))
                            continue
                        self.logger.warning(f"Invalid content: {message}")

                    elif response.status_code in ScraperConfig.RETRY_STATUSES:
                        self.logger.warning(
                            f"Retry status {response.status_code} for {url}")

                    await asyncio.sleep(ScraperConfig.RETRY_DELAY * (attempt + 1))

            except Exception as e:
                self.logger.error(f"Request failed for {url}: {str(e)}")
                if "timeout" in str(e).lower() or "connection" in str(e).lower():
                    await asyncio.sleep(ScraperConfig.RETRY_DELAY * (attempt + 1))
                    continue
                return None, None

        return None, None

    async def _process_search_page(self, session: AsyncSession, page_num: int, search_term: str) -> Tuple[bool, List[str]]:
        """Process a single search results page."""
        params = {
            "_nkw": search_term,
            "_pgn": page_num,
            "_ipg": 240
        }
        url = f"{self.proxy_manager.base_url}/sch/i.html?{urlencode(params)}"

        self.logger.info(f"Processing page {page_num} for '{search_term}'")
        status_code, content = await self._make_request(session, url, "search_page")
        if not content:
            return False, []

        soup = BeautifulSoup(content, "lxml")
        has_next, product_urls = ContentExtractor.extract_navigation_info(
            soup, self.proxy_manager.base_url)

        if product_urls:
            self.logger.info(
                f"Found {len(product_urls)} products on page {page_num}")

        return has_next, product_urls

    async def _scrape_product(self, session: AsyncSession, url: str) -> Optional[Dict]:
        """Scrape details from a single product page."""
        if url in self.scraped_urls:
            return None

        try:
            status_code, content = await self._make_request(session, url, "product_page")
            if not content:
                self.logger.warning(f"Failed to load product: {url}")
                return None

            soup = BeautifulSoup(content, "lxml")
            details = ContentExtractor.extract_product_details(soup, url)

            if not details.title or not details.current_price:
                self.logger.warning(f"Incomplete data for {url}")
                return None

            self.scraped_urls.add(url)
            self.logger.info(f"Scraped: {details.title} - {details.current_price}")
            return asdict(details)

        except Exception as e:
            self.logger.error(f"Error scraping {url}: {str(e)}")
            return None

    async def _process_product_urls(self, session: AsyncSession, urls: List[str]):
        """Process a batch of product URLs."""
        if self.max_items and self.items_scraped >= self.max_items:
            return

        tasks = []
        remaining_items = self.max_items - self.items_scraped if self.max_items else len(urls)
        for url in urls[:remaining_items]:
            if url not in self.scraped_urls:
                tasks.append(self._scrape_product(session, url))

        results = await asyncio.gather(*tasks)
        valid_results = [r for r in results if r]

        if valid_results:
            self.products.extend(valid_results)
            self.items_scraped += len(valid_results)
            await self.data_manager.save_batch(valid_results)
            self.logger.info(
                f"Progress: {self.items_scraped}/{self.max_items if self.max_items else 'unlimited'}")

    async def scrape_all(self, search_terms: List[str], output_dir: str) -> Dict[str, Dict]:
        """Main method to scrape products for multiple search terms."""
        all_results = {}

        for search_term in search_terms:
            self._initialize_state()
            output_file = str(Path(output_dir) / f"ebay_products_{search_term}.json").replace(" ", "_")
            self.data_manager = DataManager(output_file, self.max_items)

            self.logger.info(f"Starting scrape for search term: {search_term}")

            try:
                results = await self._scrape_search_term(search_term)
                all_results[search_term] = results
            except Exception as e:
                self.logger.error(f"Error scraping '{search_term}': {str(e)}")
                all_results[search_term] = {
                    "error": str(e),
                    "items_found": self.items_scraped,
                    "items_requested": self.max_items
                }

        return all_results

    async def _scrape_search_term(self, search_term: str) -> Dict:
        """Scrape products for a single search term."""
        page_num = 1

        async with AsyncSession() as session:
            while True:
                has_next, product_urls = await self._process_search_page(session, page_num, search_term)

                if not product_urls:
                    break

                await self._process_product_urls(session, product_urls)

                if not has_next or (self.max_items and self.items_scraped >= self.max_items):
                    break

                page_num += 1
                await asyncio.sleep(1)

        return {
            "items_found": self.items_scraped,
            "items_requested": self.max_items,
            "products": self.products
        }


async def main():
    """Main entry point for the scraper."""
    logger = LoggingManager.setup_logging()

    try:
        configure_system()
        os.makedirs("output", exist_ok=True)

        domain = "NL"
        print(f"\nScraping eBay Netherlands: {ScraperConfig.EBAY_DOMAINS[domain]}")

        search_terms_input = input("\nEnter search terms (comma-separated): ").strip()
        search_terms = [term.strip() for term in search_terms_input.split(",")]

        max_items_input = input("\nEnter maximum items to scrape (leave empty for all): ").strip()
        max_items = int(max_items_input) if max_items_input else None

        scraper = EbayRealTimeScraper(
            domain=domain,
            max_retries=3,
            max_concurrent_requests=3,
            batch_size=50,
            max_items=max_items
        )

        results = await scraper.scrape_all(
            search_terms=search_terms,
            output_dir="output"
        )

        print("\nResults:")
        for term, data in results.items():
            items_found = data.get("items_found", 0)
            print(f"- '{term}': {items_found} items scraped")

    except KeyboardInterrupt:
        print("\nScraping interrupted by user")
    except ValueError as ve:
        logger.error(f"Configuration error: {str(ve)}")
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
    finally:
        print("\nScraping completed. Check the output directory for results.")


if __name__ == "__main__":
    if platform.system() == "Windows":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
