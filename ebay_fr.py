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

# Constants
EBAY_DOMAIN = {
    "FR": "https://www.ebay.fr"
}


class ConfigurationError(Exception):
    """Custom exception for configuration errors"""
    pass


class ValidationError(Exception):
    """Custom exception for content validation errors"""
    pass


@dataclass
class ProductDetails:
    """Data class to store product details"""
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
    store_info: Optional[Dict[str, str]] = field(
        default_factory=lambda: {
            "name": "",
            "feedback": "",
            "sales": ""
        }
    )


class UTF8FileHandler(logging.FileHandler):
    """Custom file handler that ensures UTF-8 encoding"""

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


class FileManager:
    """Handles file operations and backup management"""

    def __init__(self, output_file: str, backup_interval: int = 20):
        self.output_file = output_file
        self.backup_interval = backup_interval
        self.save_count = 0
        self.last_saved_products = []
        self.saved_product_ids = set()

    async def save_batch(self, batch: List[Dict]):
        """Save a batch of products to file with proper error handling and backups"""
        if not batch:
            return

        try:
            os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
            self.save_count += 1

            if self.save_count % self.backup_interval == 0:
                await self.create_backup()

            existing_data = await self._read_existing_data()
            updated_data = await self._update_data(existing_data, batch)
            await self._write_data(updated_data)

        except Exception as e:
            logging.error(f"Error saving batch: {str(e)}")
            await self._create_emergency_backup(batch)

    async def _read_existing_data(self) -> Dict:
        """Read existing data from file"""
        if not os.path.exists(self.output_file):
            return {"products": [], "total_products": 0}

        try:
            async with aiofiles.open(self.output_file, "r", encoding="utf-8") as f:
                content = await f.read()
                return json.loads(content) if content else {"products": []}
        except json.JSONDecodeError:
            logging.error("Error reading existing file, starting fresh")
            return {"products": []}

    async def _update_data(self, existing_data: Dict, new_batch: List[Dict]) -> Dict:
        """Update existing data with new batch"""
        new_products = [
            product for product in new_batch
            if product["url"] not in self.saved_product_ids
        ]

        for product in new_products:
            self.saved_product_ids.add(product["url"])

        all_products = existing_data.get("products", []) + new_products
        return {"products": all_products}

    async def _write_data(self, data: Dict):
        """Write data to file safely using a temporary file"""
        temp_file = f"{self.output_file}.temp"
        async with aiofiles.open(temp_file, "w", encoding="utf-8") as f:
            await f.write(json.dumps(data, indent=2, ensure_ascii=False))
        os.replace(temp_file, self.output_file)

    async def create_backup(self):
        """Create a backup of the current data file"""
        if not os.path.exists(self.output_file):
            return

        try:
            current_data = await self._read_existing_data()
            if current_data.get("products") == self.last_saved_products:
                logging.info("No changes detected, skipping backup")
                return

            backup_file = f"{self.output_file}.backup"
            async with aiofiles.open(self.output_file, "r", encoding="utf-8") as source:
                content = await source.read()
            async with aiofiles.open(backup_file, "w", encoding="utf-8") as target:
                await target.write(content)

            logging.info(f"Backup created: {backup_file}")
            self.last_saved_products = current_data.get("products")
        except Exception as e:
            logging.error(f"Backup creation failed: {str(e)}")

    async def _create_emergency_backup(self, batch: List[Dict]):
        """Create an emergency backup in case of failure"""
        try:
            emergency_file = f"{self.output_file}.emergency_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            async with aiofiles.open(emergency_file, "w", encoding="utf-8") as f:
                await f.write(json.dumps({
                    "products": batch,
                    "emergency_backup": True,
                    "timestamp": datetime.now().isoformat()
                }, indent=2, ensure_ascii=False))
            logging.info(f"Emergency backup created: {emergency_file}")
        except Exception as e:
            logging.error(f"Emergency backup failed: {str(e)}")

class EbayParser:
    """Handles parsing of eBay pages and extraction of product details"""

    @staticmethod
    def extract_product_urls(soup: BeautifulSoup, base_url: str, scraped_urls: Set[str]) -> List[str]:
        """Extract product URLs from search results page"""
        urls = []
        for link in soup.select("a.s-item__link"):
            url = link.get("href", "")
            if url and "itm/" in url:
                try:
                    item_id = url.split("itm/")[1].split("?")[0]
                    clean_url = f"{base_url}/itm/{item_id}"
                    if clean_url not in scraped_urls:
                        urls.append(clean_url)
                except IndexError:
                    continue
        return urls

    @staticmethod
    def has_next_page(soup: BeautifulSoup) -> bool:
        """Check if there's a next page of results"""
        next_link = soup.select_one('a[type="next"]')
        next_button = soup.select_one('button[type="next"]')

        if next_button and next_button.get("aria-disabled") == "true":
            return False
        return bool(next_link and next_link.get("href"))

    @staticmethod
    def extract_store_info(soup: BeautifulSoup) -> Dict[str, str]:
        """Extract seller/store information"""
        store_info = {
            "name": "",
            "feedback": "",
            "sales": ""
        }

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
            logging.error(f"Error extracting store info: {str(e)}")

        return store_info

    @staticmethod
    def extract_shipping_info(shipping_div: BeautifulSoup) -> Tuple[Optional[str], Optional[str]]:
        """Extract shipping and location information"""
        shipping = None
        location = None

        if shipping_row := shipping_div.select_one("div.ux-labels-values--shipping"):
            shipping_text_parts = []

            if shipping_section := shipping_row.select_one("div.ux-labels-values__values-content"):
                if first_div := shipping_section.select_one("div:first-child"):
                    shipping_spans = first_div.find_all(
                        "span",
                        class_=lambda x: x and ('ux-textspans--BOLD' in x or 'ux-textspans--POSITIVE' in x)
                    )
                    shipping_text_parts.extend(
                        span.text.strip() for span in shipping_spans
                        if span.text.strip() not in ['.', '&nbsp;', ' ']
                    )

                shipping = " ".join(shipping_text_parts).strip().rstrip('.')

                if location_span := shipping_section.select_one("div:last-child span.ux-textspans--SECONDARY"):
                    location_text = location_span.text.strip()
                    location_prefixes = [
                        "Located in:",
                        "Ubicado en:",
                        "Lieu où se trouve l'objet : ",
                        "Lieu où se trouve l'objet&nbsp;:"
                    ]
                    for prefix in location_prefixes:
                        if prefix in location_text:
                            location = location_text.split(prefix)[-1].strip()
                            break
                    if not location:
                        location = location_text

        return shipping, location

class ProductDetailsExtractor:
    """Helper class to extract product details from eBay pages"""

    @staticmethod
    def extract_title_info(soup: BeautifulSoup) -> Tuple[Optional[str], Optional[str]]:
        """Extract product title and subtitle"""
        title = subtitle = None
        if title_div := soup.select_one("div.x-item-title"):
            if title_elem := title_div.select_one("h1.x-item-title__mainTitle span"):
                title = title_elem.text.strip()
            if subtitle_elem := title_div.select_one("div.x-item-title__subTitle span"):
                subtitle = subtitle_elem.text.strip()
        return title, subtitle

    @staticmethod
    def extract_price_info(soup: BeautifulSoup) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """Extract product price information"""
        current_price = was_price = discount = None
        if price_section := soup.select_one("div.x-price-section"):
            if current_price_elem := price_section.select_one("div.x-price-primary span"):
                current_price = current_price_elem.text.strip()
            if was_price_elem := price_section.select_one("span.ux-textspans--STRIKETHROUGH"):
                was_price = was_price_elem.text.strip()

            # Extract discount
            discount_text = None
            if emphasis_discount := price_section.select_one("span.ux-textspans--EMPHASIS"):
                discount_text = emphasis_discount.text.strip()
            elif secondary_discount := price_section.select_one("span.ux-textspans--SECONDARY"):
                discount_text = secondary_discount.text.strip()

            if discount_text:
                if percentage_match := re.search(r'(\d+)%', discount_text):
                    discount = percentage_match.group(1) + '%'

        return current_price, was_price, discount

    @staticmethod
    def extract_availability_info(soup: BeautifulSoup) -> Tuple[Optional[str], Optional[str]]:
        """Extract availability and sold count information"""
        availability = sold_count = None
        if quantity_div := soup.select_one("div.x-quantity__availability"):
            spans = quantity_div.select("span.ux-textspans")
            if len(spans) >= 1:
                availability = spans[0].text.strip()
            if len(spans) >= 2:
                sold_count = spans[1].text.strip()
        return availability, sold_count

    @staticmethod
    def extract_shipping_info(soup: BeautifulSoup) -> Tuple[Optional[str], Optional[str]]:
        """Extract shipping and location information"""
        shipping = "No shipping info found"
        location = None

        if shipping_div := soup.select_one("div.d-shipping-minview"):
            if shipping_row := shipping_div.select_one(
                "div.ux-layout-section__row div.ux-labels-values-with-hints[data-testid='ux-labels-values-with-hints'] "
                "div.ux-labels-values--shipping"
            ):
                if shipping_section := shipping_row.select_one("div.ux-labels-values__values-content"):
                    # Extract shipping information
                    shipping_text_parts = []
                    if first_div := shipping_section.select_one("div:first-child"):
                        shipping_spans = first_div.find_all(
                            "span",
                            class_=lambda x: x and ('ux-textspans--BOLD' in x or 'ux-textspans--POSITIVE' in x)
                        )
                        shipping_text_parts = [
                            span.text.strip() for span in shipping_spans
                            if span.text.strip() and span.text.strip() not in ['.', '&nbsp;', ' ']
                        ]

                    if shipping_text_parts:
                        shipping = " ".join(shipping_text_parts).strip().rstrip('.')

                    # Extract location
                    if location_span := shipping_section.select_one("div:last-child span.ux-textspans--SECONDARY"):
                        location_text = location_span.text.strip()
                        location_prefixes = [
                            "Located in:",
                            "Ubicado en:",
                            "Lieu où se trouve l'objet : ",
                            "Lieu où se trouve l'objet&nbsp;:"
                        ]
                        for prefix in location_prefixes:
                            if prefix in location_text:
                                location = location_text.split(prefix)[-1].strip()
                                break
                        if not location:
                            location = location_text

        return shipping, location

    @staticmethod
    def extract_returns_info(soup: BeautifulSoup) -> Optional[str]:
        """Extract returns policy information"""
        returns = None
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

                if returns_text_parts:
                    returns = " ".join(returns_text_parts).strip()
                    if "Afficher" in returns:
                        returns = returns.split("Afficher")[0].strip()
                    returns = " ".join(returns.split()).replace(" .", ".")

        return returns

    @staticmethod
    def extract_condition(soup: BeautifulSoup) -> Optional[str]:
        """Extract product condition information"""
        condition = None
        if condition_span := soup.select_one(
            "div.x-item-condition-max-view .ux-section__item > span.ux-textspans"
        ):
            condition = condition_span.text.strip().split(".")[0] + "."
        return condition

    @staticmethod
    def extract_brand(soup: BeautifulSoup) -> Optional[str]:
        """Extract brand information"""
        # Try different selectors for brand
        selectors = [
            ("dl.ux-labels-values--brand", "dd .ux-textspans"),
            ("dl.ux-labels-values--marque", "dd .ux-textspans"),
            ("div.ux-labels-values--brand", ".ux-labels-values__values .ux-textspans")
        ]

        for parent_selector, child_selector in selectors:
            if parent := soup.select_one(parent_selector):
                if brand_value := parent.select_one(child_selector):
                    return brand_value.text.strip()
        return None

    @staticmethod
    def extract_type(soup: BeautifulSoup) -> Optional[str]:
        """Extract product type/style information"""
        # Try dl selectors first
        dl_selectors = [
            "dl.ux-labels-values--type",
            "dl.ux-labels-values--style",
            "dl.ux-labels-values--kind"
        ]

        for selector in dl_selectors:
            if type_dl := soup.select_one(selector):
                if type_value := type_dl.select_one("dd .ux-textspans"):
                    return type_value.text.strip()

        # Try div selectors if dl selectors failed
        div_selectors = [
            "div.ux-labels-values--type",
            "div.ux-labels-values--style",
            "div.ux-labels-values--kind"
        ]

        for selector in div_selectors:
            if type_div := soup.select_one(selector):
                if type_value := type_div.select_one(".ux-labels-values__values .ux-textspans"):
                    return type_value.text.strip()

        return None

    @classmethod
    def extract_all_details(cls, soup: BeautifulSoup, url: str) -> ProductDetails:
        """Extract all product details from the page"""
        details = ProductDetails(url=url)

        try:
            # Extract basic information
            details.title, details.subtitle = cls.extract_title_info(soup)
            details.current_price, details.was_price, details.discount = cls.extract_price_info(soup)
            details.availability, details.sold_count = cls.extract_availability_info(soup)

            # Extract shipping and location
            details.shipping, details.location = cls.extract_shipping_info(soup)

            # Extract additional details
            details.returns = cls.extract_returns_info(soup)
            details.condition = cls.extract_condition(soup)
            details.brand = cls.extract_brand(soup)
            details.type = cls.extract_type(soup)

        except Exception as e:
            logger.error(f"Error extracting details from {url}: {str(e)}")

        return details


class EbayRealTimeScraper:
    """Main scraper class that coordinates the scraping process"""

    def __init__(
        self,
        domain: str,
        max_retries: int = 3,
        max_concurrent_requests: int = 3,
        batch_size: int = 50,
        max_items: Optional[int] = None
    ):
        self._initialize_credentials()
        self._initialize_domain(domain)
        self._initialize_settings(max_retries, max_concurrent_requests, batch_size, max_items)
        self._initialize_state()

    def _initialize_credentials(self):
        """Initialize proxy credentials"""
        self.proxy_host = "network.joinmassive.com:65534"
        self.username = os.getenv("PROXY_USERNAME")
        self.password = os.getenv("PROXY_PASSWORD")

        if not self.username or not self.password:
            raise ConfigurationError("Proxy credentials not found in environment variables")

    def _initialize_domain(self, domain: str):
        """Initialize domain-specific settings"""
        self.domain = domain
        self.base_search_url = EBAY_DOMAIN.get(domain)

        if not self.base_search_url:
            raise ConfigurationError(f"Invalid domain. Valid options: {', '.join(EBAY_DOMAIN.keys())}")

        self.base_search_url += "/sch/i.html?"
        self.proxy_auth = f"{self.username}-country-{self.domain}:{self.password}"

    def _initialize_settings(
        self,
        max_retries: int,
        max_concurrent_requests: int,
        batch_size: int,
        max_items: Optional[int]
    ):
        """Initialize scraper settings"""
        self.max_retries = max_retries
        self.max_concurrent_requests = max_concurrent_requests
        self.batch_size = batch_size
        self.max_items = max_items
        self.page_load_timeout = 30
        self.retry_delay = 5
        self.retry_statuses = {408, 429, 500, 502, 503, 504}
        self.min_content_length = 50000
        self.required_elements = {
            "search_page": ["ul.srp-results", "li.s-item"],
            "product_page": ["div.x-item-title", "div.x-price-section"]
        }

    def _initialize_state(self):
        """Initialize scraper state variables"""
        self.scraped_urls: Set[str] = set()
        self.products: List[Dict] = []
        self.product_buffer: Deque[Dict] = deque(maxlen=self.batch_size)
        self.total_saved = 0
        self.current_output_file = None
        self.items_scraped = 0
        self.semaphore = asyncio.Semaphore(self.max_concurrent_requests)
        self.parser = EbayParser()

        self.setup_signal_handlers()

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
        logging.info("Shutdown signal received. Saving remaining data...")
        if self.product_buffer and self.current_output_file:
            loop = asyncio.get_event_loop()
            loop.run_until_complete(self.force_save_buffer())
        logging.info("Shutdown complete")
        sys.exit(0)

    def validate_page_content(self, html_content: str, page_type: str) -> Tuple[bool, str]:
        """Validate page content"""
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

    async def _make_request(
        self,
        session: AsyncSession,
        url: str,
        page_type: str
    ) -> Tuple[int, Optional[str]]:
        """Make HTTP request with retries and validation"""
        retries = 0
        last_error = None

        while retries < self.max_retries:
            try:
                async with self.semaphore:
                    response = await session.get(
                        url,
                        proxies=self._get_proxy_config(),
                        impersonate="chrome124",
                        timeout=self.page_load_timeout
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
                        logging.warning(f"Attempt {retries + 1}: Got status {response.status_code}")
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

    async def process_search_page(
        self,
        session: AsyncSession,
        page_num: int,
        search_term: str
    ) -> Tuple[bool, bool]:
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
                url = self.base_search_url + urlencode(params)
                logging.info(f"Processing search page {page_num} (Attempt {retry_count + 1})")

                status_code, html_content = await self._make_request(session, url, "search_page")
                if not html_content:
                    retry_count += 1
                    await asyncio.sleep(self.retry_delay * (retry_count + 1))
                    continue

                soup = BeautifulSoup(html_content, "lxml")
                product_urls = self.parser.extract_product_urls(
                    soup,
                    self.base_search_url.split('/sch')[0],
                    self.scraped_urls
                )

                if not product_urls:
                    logging.info(f"No more products found for '{search_term}' after {self.items_scraped} items")
                    return False, True

                logging.info(f"Found {len(product_urls)} products on page {page_num}")
                await self._process_product_urls(session, product_urls)

                has_next = self.parser.has_next_page(soup)
                return (has_next and (not self.max_items or self.items_scraped < self.max_items)), False

            except Exception as e:
                logging.error(f"Error processing page {page_num}: {str(e)}")
                retry_count += 1
                await asyncio.sleep(self.retry_delay * (retry_count + 1))

        logging.error(f"Failed to process page {page_num} after all retries")
        return False, False

    async def _process_product_urls(self, session: AsyncSession, product_urls: List[str]):
        """Process a batch of product URLs"""
        chunk_size = 5
        for i in range(0, len(product_urls), chunk_size):
            if self.max_items and self.items_scraped >= self.max_items:
                logging.info(f"Reached maximum items limit ({self.max_items})")
                return

            chunk = product_urls[i:i + chunk_size]
            remaining_items = (self.max_items - self.items_scraped) if self.max_items else None
            if remaining_items:
                chunk = chunk[:remaining_items]

            tasks = [self.scrape_product(session, url) for url in chunk]
            results = await asyncio.gather(*tasks)
            valid_results = [r for r in results if r is not None]

            if valid_results:
                self.items_scraped += len(valid_results)
                self.products.extend(valid_results)
                await self.force_save_buffer()
                logging.info(
                    f"Progress: {self.items_scraped}/{self.max_items if self.max_items else 'unlimited'} items"
                )

            await asyncio.sleep(1)

    async def scrape_product(self, session: AsyncSession, url: str) -> Optional[Dict]:
        """Scrape details for a single product"""
        try:
            status_code, html_content = await self._make_request(session, url, "product_page")
            if not html_content:
                logging.error(f"Failed to load product page: {url}")
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

    async def scrape_all(self, search_terms: List[str], output_dir: str) -> Dict[str, Dict]:
        """Main method to scrape multiple search terms"""
        all_results = {}

        for search_term in search_terms:
            self._reset_scraper_state()
            output_file = Path(output_dir) / f"ebay_products_{self.domain}_{search_term}.json"
            self.current_output_file = str(output_file).replace(" ", "_")

            logging.info(f"Starting scrape for search term: {search_term}")
            results = await self._scrape_search_term(search_term)
            all_results[search_term] = results

        return all_results

    def _reset_scraper_state(self):
        """Reset scraper state for new search term"""
        self.items_scraped = 0
        self.products = []
        self.scraped_urls = set()
        self.product_buffer.clear()

    def extract_product_details(self, soup: BeautifulSoup, url: str) -> ProductDetails:
        """Extract product details using the ProductDetailsExtractor"""
        return ProductDetailsExtractor.extract_all_details(soup, url)

    async def _scrape_search_term(self, search_term: str) -> Dict:
        """Scrape products for a single search term"""
        async with AsyncSession() as session:
            page_num = 1
            try:
                while True:
                    has_next, no_more_items = await self.process_search_page(session, page_num, search_term)

                    if no_more_items:
                        logging.warning(
                            f"Search term '{search_term}' has fewer products ({self.items_scraped}) "
                            f"than requested ({self.max_items})"
                        )
                        break

                    if not has_next:
                        if self.max_items and self.items_scraped < self.max_items:
                            logging.warning(
                                f"Could only find {self.items_scraped} items for '{search_term}', "
                                f"fewer than requested {self.max_items}"
                            )
                        else:
                            logging.info(f"Completed scraping for '{search_term}' with {self.items_scraped} items")
                        break

                    page_num += 1
                    await asyncio.sleep(1)

                await self.force_save_buffer()
                return {
                    "items_found": self.items_scraped,
                    "items_requested": self.max_items,
                    "products": self.products.copy()
                }

            except Exception as e:
                logging.error(f"Error during scraping '{search_term}': {str(e)}")
                await self.force_save_buffer()
                return {
                    "items_found": self.items_scraped,
                    "items_requested": self.max_items,
                    "products": self.products.copy(),
                    "error": str(e)
                }

    async def add_to_buffer(self, product: Dict):
        """Add product to buffer and save if buffer is full"""
        self.product_buffer.append(product)
        if len(self.product_buffer) >= self.batch_size:
            await self.save_batch(list(self.product_buffer))
            self.product_buffer.clear()

    async def force_save_buffer(self):
        """Force save current buffer contents"""
        if self.product_buffer and self.current_output_file:
            await self.save_batch(list(self.product_buffer))
            self.product_buffer.clear()

    async def save_batch(self, batch: List[Dict]):
        """Save a batch of products"""
        if not batch:
            return
        file_manager = FileManager(self.current_output_file)
        await file_manager.save_batch(batch)


def setup_logging():
    """Configure logging with both file and console handlers"""
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


async def main():
    """Main entry point of the scraper"""
    try:
        os.makedirs("output", exist_ok=True)

        domain = "FR"
        print(f"Scraping eBay domain: 🌐 {domain} - {EBAY_DOMAIN[domain]}")

        search_terms = [
            term.strip()
            for term in input("\nEnter search terms (comma-separated): ").strip().split(",")
        ]
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


if __name__ == "__main__":
    if platform.system() == "Windows":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Process terminated by user")
    except Exception as e:
        logging.error(f"Fatal error: {str(e)}")
