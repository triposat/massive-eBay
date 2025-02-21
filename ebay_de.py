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

load_dotenv()

# Configure stdout encoding for Windows

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
# Constants

EBAY_DOMAIN = {"ES": "https://www.ebay.es"}

REQUIRED_ELEMENTS = {
    "search_page": ["ul.srp-results", "li.s-item"],
    "product_page": ["div.x-item-title", "div.x-price-section"],
}

ERROR_TEXTS = [
    "Sorry, the page you requested was not found",
    "This listing was ended",
    "Security Measure",
    "Robot Check",
]


class UTF8FileHandler(logging.FileHandler):
    """Custom file handler for UTF-8 encoded log files"""

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


def setup_logging():
    """Configure logging with both file and console handlers"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            UTF8FileHandler("ebay_scraper.log"),
            logging.StreamHandler(sys.stdout),
        ],
    )
    return logging.getLogger(__name__)


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
    store_info: Optional[Dict[str, str]] = field(
        default_factory=lambda: {"name": "", "feedback": "", "sales": ""}
    )


class EbayRealTimeScraper:
    def __init__(
        self,
        domain: str,
        max_retries: int = 3,
        max_concurrent_requests: int = 3,
        batch_size: int = 50,
        max_items: Optional[int] = None,
    ):
        self._initialize_proxy_settings()
        self._initialize_scraper_settings(
            domain, max_retries, max_concurrent_requests, batch_size, max_items
        )
        self._initialize_state_variables()
        self.setup_signal_handlers()

    def _initialize_proxy_settings(self):
        """Initialize proxy-related settings"""
        self.proxy_host = "network.joinmassive.com:65534"
        self.username = os.getenv("PROXY_USERNAME")
        self.password = os.getenv("PROXY_PASSWORD")

        if not self.username or not self.password:
            raise ValueError("Proxy credentials not found in environment variables")
        self.proxy_auth = f"{self.username}-country-ES:{self.password}"

    def _initialize_scraper_settings(
        self, domain, max_retries, max_concurrent_requests, batch_size, max_items
    ):
        """Initialize scraper-specific settings"""
        self.domain = domain
        self.base_search_url = EBAY_DOMAIN.get(domain)

        if not self.base_search_url:
            raise ValueError(
                f"Invalid domain identifier. Valid options are: {
                             ', '.join(EBAY_DOMAIN.keys())}"
            )
        self.base_search_url += "/sch/i.html?"
        self.max_retries = max_retries
        self.max_concurrent_requests = max_concurrent_requests
        self.batch_size = batch_size
        self.max_items = max_items
        self.page_load_timeout = 30
        self.retry_delay = 5
        self.retry_statuses = {408, 429, 500, 502, 503, 504}
        self.min_content_length = 50000

    def _initialize_state_variables(self):
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
        """Get proxy configuration dictionary"""
        return {"https": f"http://{self.proxy_auth}@{self.proxy_host}"}

    def setup_signal_handlers(self):
        """Set up signal handlers for graceful shutdown"""
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

    async def validate_page_content(
        self, html_content: str, page_type: str
    ) -> Tuple[bool, str]:
        """Validate the content of a scraped page"""
        if not html_content or len(html_content) < self.min_content_length:
            return False, "Content too short"
        soup = BeautifulSoup(html_content, "lxml")

        for error in ERROR_TEXTS:
            if error.lower() in html_content.lower():
                return False, f"Error page detected: {error}"
        for selector in REQUIRED_ELEMENTS[page_type]:
            if not soup.select(selector):
                return False, f"Missing required element: {selector}"
        return True, "Page content valid"

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
            logger.error(f"Error extracting store information: {str(e)}")
        return store_info

    async def _make_request(
        self, session: AsyncSession, url: str, page_type: str
    ) -> Tuple[int, Optional[str]]:
        """Make an HTTP request with retries"""
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
                        is_valid, message = await self.validate_page_content(
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

    def extract_product_details(self, soup: BeautifulSoup, url: str) -> ProductDetails:
        """Extract product details from the page"""
        details = ProductDetails(url=url)

        try:
            details.store_info = self._extract_store_info(soup)
            self._extract_title_and_subtitle(soup, details)
            self._extract_price_information(soup, details)
            self._extract_quantity_info(soup, details)
            self._extract_shipping_info(soup, details)
            self._extract_returns_info(soup, details)
            self._extract_additional_details(soup, details)
        except Exception as e:
            logger.error(f"Error extracting details from {url}: {str(e)}")
        return details

    def _extract_title_and_subtitle(self, soup: BeautifulSoup, details: ProductDetails):
        """Extract title and subtitle information"""
        if title_div := soup.select_one("div.x-item-title"):
            if title := title_div.select_one("h1.x-item-title__mainTitle span"):
                details.title = title.text.strip()
            if subtitle := title_div.select_one("div.x-item-title__subTitle span"):
                details.subtitle = subtitle.text.strip()

    def _extract_price_information(self, soup: BeautifulSoup, details: ProductDetails):
        """Extract price-related information"""
        if price_section := soup.select_one("div.x-price-section"):
            if current_price := price_section.select_one("div.x-price-primary span"):
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

    def _extract_quantity_info(self, soup: BeautifulSoup, details: ProductDetails):
        """Extract quantity and availability information"""
        if quantity_div := soup.select_one("div.x-quantity__availability"):
            spans = quantity_div.select("span.ux-textspans")
            if len(spans) >= 1:
                details.availability = spans[0].text.strip()
            if len(spans) >= 2:
                details.sold_count = spans[1].text.strip()

    def _extract_shipping_info(self, soup: BeautifulSoup, details: ProductDetails):
        """Extract shipping information"""
        if shipping_div := soup.select_one("div.d-shipping-minview"):
            if shipping_row := shipping_div.select_one(
                "div.ux-layout-section__row div.ux-labels-values-with-hints[data-testid='ux-labels-values-with-hints'] div.ux-labels-values--shipping"
            ):
                self._process_shipping_section(shipping_row, details)

    def _process_shipping_section(
        self, shipping_row: BeautifulSoup, details: ProductDetails
    ):
        """Process shipping section details"""
        shipping_text_parts = []

        if shipping_section := shipping_row.select_one(
            "div.ux-labels-values__values-content"
        ):
            if first_div := shipping_section.find("div"):
                for span in first_div.find_all(
                    "span", class_="ux-textspans", recursive=False
                ):
                    if (
                        span.text.strip()
                        and not span.select_one(".ux-bubble-help")
                        and "See details" not in span.text
                        and "Ver detalles" not in span.text
                        and not span.find("span", class_="clipped")
                        and not span.get("class", [""])[0]
                        == "ux-textspans__custom-view"
                    ):

                        text = span.text.strip()
                        if text and text not in [".", "&nbsp;", " "]:
                            shipping_text_parts.append(text)
            details.shipping = " ".join(shipping_text_parts).strip()
            if details.shipping.endswith("."):
                details.shipping = details.shipping[:-1]
            # Extract location

            if location_div := shipping_section.select_one("div:nth-child(2)"):
                if location_span := location_div.select_one(
                    "span.ux-textspans--SECONDARY"
                ):
                    location_text = location_span.text.strip()
                    if "Located in:" in location_text:
                        details.location = location_text.split("Located in:")[
                            -1
                        ].strip()
                    elif "Ubicado en:" in location_text:
                        details.location = location_text.split("Ubicado en:")[
                            -1
                        ].strip()
                    else:
                        details.location = location_text
        else:
            details.shipping = "No shipping info found"
            details.location = None

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
                            if "ux-textspans--EMPHASIS" in span.get("class", []):
                                text = f"{text}"
                            returns_text_parts.append(text)
                returns_text = " ".join(returns_text_parts).strip()
                returns_text = " ".join(returns_text.split())
                returns_text = returns_text.replace(" .", ".")
                details.returns = returns_text

    def _extract_additional_details(self, soup: BeautifulSoup, details: ProductDetails):
        """Extract condition, brand, and type information"""
        # Extract condition

        if condition_span := soup.select_one(
            "div.x-item-condition-max-view .ux-section__item > span.ux-textspans"
        ):
            details.condition = condition_span.text.strip().split(".")[0] + "."
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
        """Check if there is a next page of results"""
        next_link = soup.select_one('a[type="next"]')
        if next_link and next_link.get("href"):
            return True
        next_button = soup.select_one('button[type="next"]')
        if next_button and next_button.get("aria-disabled") == "true":
            return False
        return bool(next_link)

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

    async def save_batch(self, batch: List[Dict]):
        """Save a batch of products to file"""
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
                product
                for product in batch
                if product["url"] not in self.saved_product_ids
            ]

            for product in new_products:
                self.saved_product_ids.add(product["url"])
            all_products = existing_data.get("products", []) + new_products

            await self._save_to_file(temp_file, all_products)
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

    async def _save_to_file(self, filename: str, products: List[Dict]):
        """Save products to file"""
        updated_data = {"products": products}
        async with aiofiles.open(filename, "w", encoding="utf-8") as f:
            await f.write(json.dumps(updated_data, indent=2, ensure_ascii=False))

    async def _create_emergency_backup(self, batch: List[Dict]):
        """Create emergency backup in case of save failure"""
        try:
            emergency_backup = f"{self.current_output_file}.emergency_{
                datetime.now().strftime('%Y%m%d_%H%M%S')}"
            async with aiofiles.open(emergency_backup, "w", encoding="utf-8") as f:
                await f.write(
                    json.dumps(
                        {
                            "products": batch,
                            "total_products": len(batch),
                            "requested_items": self.max_items,
                            "actual_items_found": len(batch),
                            "last_updated": datetime.now().isoformat(),
                        },
                        indent=2,
                        ensure_ascii=False,
                    )
                )
            logger.info(f"Created emergency backup: {emergency_backup}")
        except Exception as backup_error:
            logger.error(f"Emergency backup failed: {str(backup_error)}")

    async def create_backup(self):
        """Create a backup of the current data"""
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
                async with aiofiles.open(backup_file, "w", encoding="utf-8") as target:
                    await target.write(current_content)
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
                return await self._process_search_results(
                    session, html_content, search_term
                )
            except Exception as e:
                logger.error(f"Error processing page {page_num}: {str(e)}")
                retry_count += 1
                await asyncio.sleep(self.retry_delay * (retry_count + 1))
        logger.error(f"Failed to process page {page_num} after all retries")
        return False, False


    async def _process_search_results(
        self, session: AsyncSession, html_content: str, search_term: str
    ) -> Tuple[bool, bool]:
        """Process the search results from a page"""
        soup = BeautifulSoup(html_content, "lxml")
        product_urls = self.extract_product_urls(soup)

        if not product_urls:
            logger.info(
                f"No more products found for '{
                    search_term}' after {self.items_scraped} items"
            )
            return False, True
        logger.info(f"Found {len(product_urls)} products on this page")

        chunk_size = 5
        for i in range(0, len(product_urls), chunk_size):
            if self.max_items and self.items_scraped >= self.max_items:
                logger.info(f"Reached maximum items limit ({self.max_items})")
                return False, False
            chunk = product_urls[i : i + chunk_size]
            if self.max_items:
                remaining_items = self.max_items - self.items_scraped
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
        has_next = self.has_next_page(soup)
        return (
            has_next and (not self.max_items or self.items_scraped < self.max_items),
            False,
        )


    async def scrape_all(self, search_terms: List[str], output_dir: str) -> Dict[str, Dict]:
        """Main method to scrape all products for given search terms"""
        all_results = {}

        for search_term in search_terms:
            self._reset_scraper_state()

            output_file = (
                Path(output_dir) / f"ebay_products_{self.domain}_{search_term}.json"
            )
            self.current_output_file = str(output_file).replace(" ", "_")

            logger.info(f"Starting scrape for search term: {search_term}")

            async with AsyncSession() as session:
                all_results[search_term] = await self._scrape_search_term(
                    session, search_term
                )
        return all_results


    def _reset_scraper_state(self):
        """Reset scraper state for new search term"""
        self.items_scraped = 0
        self.products = []
        self.scraped_urls = set()
        self.product_buffer.clear()


    async def _scrape_search_term(self, session: AsyncSession, search_term: str) -> Dict:
        """Scrape all products for a single search term"""
        page_num = 1
        try:
            while True:
                has_next, no_more_items = await self.process_search_page(
                    session, page_num, search_term
                )

                if no_more_items:
                    logger.warning(
                        f"Search term '{search_term}' has fewer products ({
                            self.items_scraped}) than requested ({self.max_items})"
                    )
                    break
                if not has_next:
                    if self.max_items and self.items_scraped < self.max_items:
                        logger.warning(
                            f"Could only find {self.items_scraped} items for '{
                                search_term}', fewer than requested {self.max_items}"
                        )
                    else:
                        logger.info(
                            f"Completed scraping for '{search_term}' with {
                                self.items_scraped} items"
                        )
                    break
                page_num += 1
                await asyncio.sleep(1)
            await self.force_save_buffer()
            return {
                "items_found": self.items_scraped,
                "items_requested": self.max_items,
                "products": self.products.copy(),
            }
        except Exception as e:
            logger.error(f"Error during scraping '{search_term}': {str(e)}")
            await self.force_save_buffer()
            return {
                "items_found": self.items_scraped,
                "items_requested": self.max_items,
                "products": self.products.copy(),
                "error": str(e),
            }


def setup_windows_policy():
    """Set up Windows event loop policy if on Windows"""
    if platform.system() == "Windows":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


async def get_user_input() -> Tuple[List[str], Optional[int]]:
    """Get search terms and max items from user"""
    search_terms = [
        term.strip()
        for term in input("\nEnter search terms (comma-separated): ").strip().split(",")
    ]

    max_items_input = input(
        "\nEnter maximum items to scrape (leave empty for all): ").strip()
    max_items = int(max_items_input) if max_items_input else None

    return search_terms, max_items

logger = setup_logging()


async def main():
    """Main entry point of the scraper"""
    try:
        os.makedirs("output", exist_ok=True)
        domain = "ES"
        print(f"Scraping eBay domain: 🌐 {domain} - {EBAY_DOMAIN[domain]}")

        search_terms, max_items = await get_user_input()

        scraper = EbayRealTimeScraper(
            domain=domain,
            max_retries=3,
            max_concurrent_requests=3,
            batch_size=50,
            max_items=max_items
        )

        results = await scraper.scrape_all(search_terms=search_terms, output_dir="output")

        for term, data in results.items():
            logger.info(
                f"Scraped {len(data['products'])} products for '{term}'")

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
    setup_windows_policy()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Process terminated by user")
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
