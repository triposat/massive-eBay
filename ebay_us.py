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

# Configure stdout encoding for Windows
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

# Initialize logger
logger = logging.getLogger(__name__)

# Constants
EBAY_DOMAIN = {"US": "https://www.ebay.com"}

PAGE_ELEMENTS = {
    "search": ["ul.srp-results", "li.s-item"],
    "product": ["div.x-item-title", "div.x-price-section"],
}

ERROR_MESSAGES = [
    "Sorry, the page you requested was not found",
    "This listing was ended",
    "Security Measure",
    "Robot Check",
]


@dataclass
class ProductDetails:
    """Structured container for product information"""
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
    """Handles file operations for data persistence"""

    @staticmethod
    async def save_to_file(filename: str, data: Dict):
        """Save data to file in JSON format"""
        async with aiofiles.open(filename, "w", encoding="utf-8") as f:
            await f.write(json.dumps(data, indent=2, ensure_ascii=False))

    @staticmethod
    async def load_existing_data(filename: str) -> Dict:
        """Load and parse existing JSON data"""
        default_data = {
            "products": [],
            "total_products": 0,
            "requested_items": None,
            "actual_items_found": 0,
        }

        if not os.path.exists(filename):
            return default_data

        try:
            async with aiofiles.open(filename, "r", encoding="utf-8") as f:
                content = await f.read()
                return json.loads(content) if content else default_data
        except json.JSONDecodeError:
            logger.error("Error reading existing file, starting fresh")
            return default_data


class DataExtractor:
    """Handles extraction of data from HTML content"""

    @staticmethod
    def extract_shipping_info(section) -> Tuple[str, Optional[str]]:
        """Extract shipping details and location"""
        shipping_parts = []
        location = None

        if first_div := section.select("div"):
            spans = first_div[0].find_all("span", class_="ux-textspans")
            for span in spans:
                if (span.text.strip() and
                    'clipped' not in span.get('class', []) and
                    'ux-textspans__custom-view' not in span.get('class', []) and
                        'See details' not in span.text):
                    text = span.text.strip()
                    if text not in ['.', '&nbsp;', ' ']:
                        shipping_parts.append(text)

        shipping = " ".join(shipping_parts).strip()
        if shipping.endswith('.'):
            shipping = shipping[:-1]

        if location_divs := section.find_all("div"):
            for div in location_divs:
                if loc_span := div.find("span", class_="ux-textspans--SECONDARY"):
                    location = loc_span.text.strip()
                    if ":" in location:
                        location = location.split(": ", 1)[-1]

        return shipping, location

    @staticmethod
    def extract_returns_info(section) -> str:
        """Extract returns policy information"""
        returns_parts = []
        spans = section.find_all("span", class_="ux-textspans")

        for span in spans:
            if (span.text.strip() and
                'clipped' not in span.get('class', []) and
                    'See details' not in span.text):
                text = span.text.strip()
                if text not in ['.', '&nbsp;', ' ']:
                    if 'ux-textspans--EMPHASIS' in span.get('class', []):
                        text = f"{text}"
                    returns_parts.append(text)

        returns_text = " ".join(returns_parts).strip()
        returns_text = " ".join(returns_text.split())
        returns_text = returns_text.replace(" .", ".")

        return returns_text

    @staticmethod
    def extract_store_info(soup: BeautifulSoup) -> Dict[str, str]:
        """Extract seller/store information"""
        store_info = {"name": "", "feedback": "", "sales": ""}

        try:
            if store_header := soup.select_one("div.x-store-information__header"):
                if store_name := store_header.select_one("span.ux-textspans--BOLD"):
                    store_info["name"] = store_name.text.strip()

                if highlights := store_header.select_one("h4.x-store-information__highlights"):
                    for span in highlights.select("span.ux-textspans"):
                        if "positive feedback" in span.text:
                            store_info["feedback"] = span.text.strip()
                            break

                    sales_spans = highlights.select(
                        "span.ux-textspans--SECONDARY")
                    if len(sales_spans) >= 2:
                        count = sales_spans[0].text.strip()
                        text = sales_spans[1].text.strip()
                        store_info["sales"] = f"{count} {text}"

        except Exception as e:
            logger.error(f"Error extracting store information: {str(e)}")

        return store_info


class EbayRealTimeScraper:
    """Main scraper class handling eBay product data collection"""

    def __init__(
        self,
        domain: str,
        max_retries: int = 3,
        max_concurrent_requests: int = 3,
        batch_size: int = 50,
        max_items: Optional[int] = None,
    ):
        self.setup_config(domain, max_retries,
                          max_concurrent_requests, batch_size, max_items)
        self.initialize_state()
        self.setup_signal_handlers()

    def setup_config(self, domain, max_retries, max_concurrent_requests, batch_size, max_items):
        """Initialize configuration settings"""
        self.domain = domain
        self.base_url = EBAY_DOMAIN.get(domain, "")
        if not self.base_url:
            raise ValueError(f"Invalid domain. Valid options: {', '.join(EBAY_DOMAIN.keys())}")

        self.base_url += "/sch/i.html?"
        self.setup_proxy()

        # Core settings
        self.max_retries = max_retries
        self.max_concurrent_requests = max_concurrent_requests
        self.batch_size = batch_size
        self.max_items = max_items

        # Request settings
        self.page_timeout = 30
        self.retry_delay = 5
        self.retry_status_codes = {408, 429, 500, 502, 503, 504}
        self.min_content_length = 50000
        self.backup_interval = 20

    def setup_proxy(self):
        """Configure proxy settings"""
        self.proxy_host = "network.joinmassive.com:65534"
        self.username = os.getenv("PROXY_USERNAME")
        self.password = os.getenv("PROXY_PASSWORD")

        if not self.username or not self.password:
            raise ValueError(
                "Missing proxy credentials in environment variables")

        self.proxy_auth = f"{self.username}-country-{self.domain}:{self.password}"

    def initialize_state(self):
        """Initialize state tracking variables"""
        self.scraped_urls = set()
        self.products = []
        self.product_buffer = deque(maxlen=self.batch_size)
        self.total_saved = 0
        self.current_output_file = None
        self.last_saved_products = []
        self.semaphore = asyncio.Semaphore(self.max_concurrent_requests)
        self.items_scraped = 0
        self.save_count = 0
        self.saved_product_ids = set()

    def setup_signal_handlers(self):
        """Set up signal handlers for graceful shutdown"""
        try:
            for sig in (signal.SIGTERM, signal.SIGINT):
                signal.signal(sig, self._handle_shutdown)
        except AttributeError:
            pass

    def _handle_shutdown(self, signum, frame):
        """Handle shutdown signals"""
        logger.info("Shutdown signal received. Saving remaining data...")
        if self.product_buffer and self.current_output_file:
            loop = asyncio.get_event_loop()
            loop.run_until_complete(self.force_save_buffer())
        logger.info("Shutdown complete")
        sys.exit(0)

    def _get_proxy_config(self) -> Dict[str, str]:
        """Get proxy configuration"""
        return {"https": f"http://{self.proxy_auth}@{self.proxy_host}"}

    async def _make_request(self, session: AsyncSession, url: str, page_type: str) -> Tuple[int, Optional[str]]:
        """Make HTTP request with retry logic"""
        retries = 0
        last_error = None

        while retries < self.max_retries:
            try:
                async with self.semaphore:
                    response = await session.get(
                        url,
                        proxies=self._get_proxy_config(),
                        impersonate="chrome124",
                        timeout=self.page_timeout,
                    )

                    if response.status_code == 200:
                        content = response.text
                        is_valid, message = self._validate_content(
                            content, page_type)

                        if is_valid:
                            return response.status_code, content
                        else:
                            logger.warning(f"Attempt {retries + 1}: {message}")
                            if any(error in message for error in ["Robot Check", "Security Measure"]):
                                await asyncio.sleep(self.retry_delay * (retries + 1))
                                retries += 1
                                continue
                    elif response.status_code in self.retry_status_codes:
                        logger.warning(f"Attempt {retries + 1}: Status {response.status_code}")
                        await asyncio.sleep(self.retry_delay * (retries + 1))
                        retries += 1
                        continue
                    else:
                        return response.status_code, None

            except Exception as e:
                last_error = str(e)
                logger.error(f"Request failed (attempt {retries + 1}): {last_error}")
                if any(term in last_error.lower() for term in ["timeout", "connection"]):
                    await asyncio.sleep(self.retry_delay * (retries + 1))
                    retries += 1
                    continue
                return None, None

            retries += 1
            await asyncio.sleep(self.retry_delay * retries)

        logger.error(f"All retries failed. Last error: {last_error}")
        return None, None

    def _validate_content(self, html: str, page_type: str) -> Tuple[bool, str]:
        """Validate HTML content"""
        if not html or len(html) < self.min_content_length:
            return False, "Content too short"

        soup = BeautifulSoup(html, "lxml")

        for error in ERROR_MESSAGES:
            if error.lower() in html.lower():
                return False, f"Error page: {error}"

        for selector in PAGE_ELEMENTS[page_type]:
            if not soup.select(selector):
                return False, f"Missing element: {selector}"

        return True, "Content valid"

    async def scrape_all(self, search_terms: List[str], output_dir: str) -> Dict[str, Dict]:
        """Main entry point for scraping multiple search terms"""
        results = {}

        for term in search_terms:
            self._initialize_scrape_session(term, output_dir)
            logger.info(f"Starting scrape for: {term}")

            async with AsyncSession() as session:
                results[term] = await self._scrape_term(session, term)

        return results

    def _initialize_scrape_session(self, term: str, output_dir: str):
        """Initialize a new scraping session"""
        self.items_scraped = 0
        self.products = []
        self.scraped_urls = set()
        self.product_buffer.clear()

        filename = f"ebay_products_{self.domain}_{term}.json"
        self.current_output_file = str(
            Path(output_dir) / filename).replace(" ", "_")

    async def _scrape_term(self, session: AsyncSession, term: str) -> Dict:
        """Scrape products for a single search term"""
        try:
            page = 1
            while True:
                has_next, no_items = await self._process_search_page(session, page, term)

                if no_items:
                    logger.warning(f"'{term}' has fewer products ({self.items_scraped}) than requested ({self.max_items})")
                    break

                if not has_next:
                    if self.max_items and self.items_scraped < self.max_items:
                        logger.warning(f"Only found {self.items_scraped} items for '{term}', wanted {self.max_items}")
                    else:
                        logger.info(f"Completed '{term}' with {self.items_scraped} items")
                    break

                page += 1
                await asyncio.sleep(1)

            await self.force_save_buffer()
            return {
                "items_found": self.items_scraped,
                "items_requested": self.max_items,
                "products": self.products.copy(),
            }

        except Exception as e:
            logger.error(f"Error scraping '{term}': {str(e)}")
            await self.force_save_buffer()
            return {
                "items_found": self.items_scraped,
                "items_requested": self.max_items,
                "products": self.products.copy(),
                "error": str(e),
            }

    async def _process_search_page(self, session: AsyncSession, page_num: int, search_term: str) -> Tuple[bool, bool]:
        """Process a single search results page"""
        retry_count = 0
        max_page_retries = 2

        while retry_count < max_page_retries:
            try:
                params = {
                    "_nkw": search_term,
                    "_pgn": page_num,
                    "_ipg": 240,
                }
                url = self.base_url + urlencode(params)
                logger.info(f"Processing page {page_num} (Attempt {retry_count + 1})")

                status_code, html_content = await self._make_request(session, url, "search")

                if not html_content:
                    retry_count += 1
                    await asyncio.sleep(self.retry_delay * (retry_count + 1))
                    continue

                return await self._extract_page_data(session, html_content, search_term, page_num)

            except Exception as e:
                logger.error(f"Error on page {page_num}: {str(e)}")
                retry_count += 1
                await asyncio.sleep(self.retry_delay * (retry_count + 1))

        logger.error(f"Failed to process page {page_num}")
        return False, False

    async def _extract_page_data(self, session: AsyncSession, html_content: str, search_term: str, page_num: int) -> Tuple[bool, bool]:
        """Extract data from search results page"""
        soup = BeautifulSoup(html_content, "lxml")
        urls = self._extract_product_urls(soup)

        if not urls:
            logger.info(f"No more products for '{search_term}' after {self.items_scraped} items")
            return False, True

        logger.info(f"Found {len(urls)} products on page {page_num}")
        await self._process_product_batch(session, urls)

        has_next = self._has_next_page(soup)
        return (has_next and (not self.max_items or self.items_scraped < self.max_items)), False

    def _extract_product_urls(self, soup: BeautifulSoup) -> List[str]:
        """Extract product URLs from search results"""
        urls = []
        for link in soup.select("a.s-item__link"):
            url = link.get("href", "")
            if url and "itm/" in url:
                try:
                    item_id = url.split("itm/")[1].split("?")[0]
                    clean_url = f"{self.base_url.split('/sch')[0]}/itm/{item_id}"
                    if clean_url not in self.scraped_urls:
                        urls.append(clean_url)
                except IndexError:
                    continue
        return urls

    def _has_next_page(self, soup: BeautifulSoup) -> bool:
        """Check for next page of results"""
        next_link = soup.select_one('a[type="next"]')
        if next_link and next_link.get("href"):
            return True
        next_button = soup.select_one('button[type="next"]')
        return not (next_button and next_button.get("aria-disabled") == "true")

    async def _process_product_batch(self, session: AsyncSession, urls: List[str]):
        """Process a batch of product URLs"""
        chunk_size = 5
        for i in range(0, len(urls), chunk_size):
            if self.max_items and self.items_scraped >= self.max_items:
                logger.info(f"Reached limit ({self.max_items} items)")
                return

            chunk = urls[i:i + chunk_size]
            if self.max_items:
                remaining = self.max_items - self.items_scraped
                chunk = chunk[:remaining]

            tasks = [self._scrape_product(session, url) for url in chunk]
            results = await asyncio.gather(*tasks)
            valid_results = [r for r in results if r is not None]

            if valid_results:
                self.items_scraped += len(valid_results)
                self.products.extend(valid_results)
                await self.force_save_buffer()
                logger.info(f"Progress: {self.items_scraped}/{self.max_items if self.max_items else 'unlimited'}")

            await asyncio.sleep(1)

    async def _scrape_product(self, session: AsyncSession, url: str) -> Optional[Dict]:
        """Scrape details for a single product"""
        try:
            status_code, html_content = await self._make_request(session, url, "product")

            if not html_content:
                logger.error(f"Failed to load: {url}")
                return None

            soup = BeautifulSoup(html_content, "lxml")
            details = self._extract_product_details(soup, url)

            if not details.title or not details.current_price:
                logger.warning(f"Incomplete data: {url}")
                return None

            self.scraped_urls.add(url)
            product_dict = asdict(details)
            await self._add_to_buffer(product_dict)

            logger.info(f"Scraped: {details.title}")
            return product_dict

        except Exception as e:
            logger.error(f"Error scraping {url}: {str(e)}")
            return None

    def _extract_product_details(self, soup: BeautifulSoup, url: str) -> ProductDetails:
        """Extract all product details from page"""
        details = ProductDetails(url=url)

        try:
            details.store_info = DataExtractor.extract_store_info(soup)

            # Title section
            if title_div := soup.select_one("div.x-item-title"):
                if title := title_div.select_one("h1.x-item-title__mainTitle span"):
                    details.title = title.text.strip()
                if subtitle := title_div.select_one("div.x-item-title__subTitle span"):
                    details.subtitle = subtitle.text.strip()

            # Price section
            if price_section := soup.select_one("div.x-price-section"):
                if current_price := price_section.select_one("div.x-price-primary span"):
                    details.current_price = current_price.text.strip()
                if was_price := price_section.select_one("span.ux-textspans--STRIKETHROUGH"):
                    details.was_price = was_price.text.strip()

                # Discount calculation
                discount = None
                if emphasis_discount := price_section.select_one("span.ux-textspans--EMPHASIS"):
                    discount = emphasis_discount.text.strip()
                elif secondary_discount := price_section.select_one("span.ux-textspans--SECONDARY"):
                    discount = secondary_discount.text.strip()

                if discount and (percentage_match := re.search(r'(\d+)%', discount)):
                    details.discount = percentage_match.group(1) + '%'

            # Quantity section
            if quantity_div := soup.select_one("div.x-quantity__availability"):
                spans = quantity_div.select("span.ux-textspans")
                if spans:
                    details.availability = spans[0].text.strip()
                    if len(spans) > 1:
                        details.sold_count = spans[1].text.strip()

            # Shipping section
            if shipping_div := soup.select_one("div.d-shipping-minview"):
                if shipping_section := shipping_div.select_one("div.ux-labels-values__values-content"):
                    details.shipping, details.location = DataExtractor.extract_shipping_info(
                        shipping_section)

            # Returns section
            if returns_div := soup.select_one("div.x-returns-minview"):
                if returns_section := returns_div.select_one("div.ux-labels-values__values-content"):
                    details.returns = DataExtractor.extract_returns_info(
                        returns_section)

            # Additional details
            if condition_span := soup.select_one("div.x-item-condition-max-view .ux-section__item > span.ux-textspans"):
                details.condition = condition_span.text.strip().split(".")[
                    0] + "."

            if (brand_dl := soup.select_one("dl.ux-labels-values--brand")) and (brand_value := brand_dl.select_one("dd .ux-textspans")):
                details.brand = brand_value.text.strip()

            if (type_dl := soup.select_one("dl.ux-labels-values--type")) and (type_value := type_dl.select_one("dd .ux-textspans")):
                details.type = type_value.text.strip()

        except Exception as e:
            logger.error(f"Error extracting details from {url}: {str(e)}")

        return details

    async def _add_to_buffer(self, product: Dict):
        """Add product to buffer and save if full"""
        self.product_buffer.append(product)
        if len(self.product_buffer) >= self.batch_size:
            await self._save_batch(list(self.product_buffer))
            self.product_buffer.clear()

    async def force_save_buffer(self):
        """Force save current buffer contents"""
        if self.product_buffer and self.current_output_file:
            await self._save_batch(list(self.product_buffer))
            self.product_buffer.clear()

    async def _save_batch(self, batch: List[Dict]):
        """Save a batch of products to file"""
        if not batch:
            return

        try:
            os.makedirs(os.path.dirname(
                self.current_output_file), exist_ok=True)
            self.save_count += 1

            if self.save_count % self.backup_interval == 0:
                await self._create_backup()

            temp_file = f"{self.current_output_file}.temp"
            existing_data = await FileHandler.load_existing_data(self.current_output_file)

            new_products = [
                product for product in batch
                if product["url"] not in self.saved_product_ids
            ]

            for product in new_products:
                self.saved_product_ids.add(product["url"])

            all_products = existing_data.get("products", []) + new_products
            updated_data = {
                "products": all_products,
                "total_products": len(all_products),
                "requested_items": self.max_items,
                "actual_items_found": len(all_products),
                "last_updated": datetime.now().isoformat()
            }

            await FileHandler.save_to_file(temp_file, updated_data)
            os.replace(temp_file, self.current_output_file)

            self.total_saved = len(all_products)
            logger.info(f"Saved {len(new_products)} new products. Total: {self.total_saved}")

        except Exception as e:
            logger.error(f"Error saving batch: {str(e)}")
            await self._create_emergency_backup(batch)

    async def _create_backup(self):
        """Create backup of current data"""
        if os.path.exists(self.current_output_file):
            try:
                current_data = await FileHandler.load_existing_data(self.current_output_file)

                if current_data.get("products") == self.last_saved_products:
                    logger.info("No changes detected, skipping backup")
                    return

                backup_file = f"{self.current_output_file}.backup"
                await FileHandler.save_to_file(backup_file, current_data)

                logger.info(f"Backup created: {backup_file}")
                self.last_saved_products = current_data.get("products")

            except Exception as e:
                logger.error(f"Backup failed: {str(e)}")

    async def _create_emergency_backup(self, batch: List[Dict]):
        """Create emergency backup in case of save failure"""
        try:
            emergency_file = f"{self.current_output_file}.emergency_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            emergency_data = {
                "products": batch,
                "total_products": len(batch),
                "requested_items": self.max_items,
                "actual_items_found": len(batch),
                "last_updated": datetime.now().isoformat(),
            }
            await FileHandler.save_to_file(emergency_file, emergency_data)
            logger.info(f"Emergency backup created: {emergency_file}")
        except Exception as e:
            logger.error(f"Emergency backup failed: {str(e)}")


async def main():
    """Main entry point"""
    logger = setup_logging()

    try:
        os.makedirs("output", exist_ok=True)

        # Set Windows event loop policy if needed
        if platform.system() == "Windows":
            asyncio.set_event_loop_policy(
                asyncio.WindowsSelectorEventLoopPolicy())

        domain = "US"
        print(f"Scraping eBay domain: 🌐 {domain} - {EBAY_DOMAIN[domain]}")

        # Get user inputs
        search_terms = [term.strip() for term in input(
            "\nEnter search terms (comma-separated): ").strip().split(",")]
        max_items_input = input(
            "\nEnter maximum items to scrape (leave empty for all): ").strip()
        max_items = int(max_items_input) if max_items_input else None

        # Initialize and run scraper
        scraper = EbayRealTimeScraper(
            domain=domain,
            max_retries=3,
            max_concurrent_requests=3,
            batch_size=50,
            max_items=max_items
        )

        results = await scraper.scrape_all(search_terms=search_terms, output_dir="output")

        # Log results
        for term, data in results.items():
            logger.info(f"Scraped {len(data['products'])} products for '{term}'")

    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user")
        if "scraper" in locals():
            await scraper.force_save_buffer()
    except Exception as e:
        logger.error(f"Fatal error in main: {str(e)}")
        if "scraper" in locals():
            await scraper.force_save_buffer()
    finally:
        logger.info("Scraping completed. Check output directory for results.")


def setup_logging():
    """Configure logging settings"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler("ebay_scraper.log", encoding="utf-8"),
            logging.StreamHandler(sys.stdout)
        ],
    )
    return logging.getLogger(__name__)


if __name__ == "__main__":
    if platform.system() == "Windows":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Process terminated by user")
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        sys.exit(1)
