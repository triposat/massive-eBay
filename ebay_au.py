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
    "AU": "https://www.ebay.com.au",
}

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


class ConfigurationError(Exception):
    """Custom exception for configuration errors."""
    pass


@dataclass
class ProductDetails:
    """Data class to store product details."""
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


class LoggingConfig:
    """Configure logging settings."""
    @staticmethod
    def setup_logging():
        class UTF8FileHandler(logging.FileHandler):
            def __init__(self, filename, mode="a", encoding="utf-8"):
                super().__init__(filename, mode, encoding)

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[
                UTF8FileHandler("ebay_scraper.log"),
                logging.StreamHandler(sys.stdout)
            ]
        )
        return logging.getLogger(__name__)


class ProxyConfig:
    """Handle proxy configuration."""

    def __init__(self, domain: str):
        self.proxy_host = "network.joinmassive.com:65534"
        self.username = os.getenv("PROXY_USERNAME")
        self.password = os.getenv("PROXY_PASSWORD")
        self.domain = domain

        if not self.username or not self.password:
            raise ConfigurationError(
                "Proxy credentials not found in environment variables")

        self.proxy_auth = f"{
            self.username}-country-{self.domain}:{self.password}"

    def get_proxy_config(self) -> Dict[str, str]:
        return {"https": f"http://{self.proxy_auth}@{self.proxy_host}"}


class EbayRealTimeScraper:
    """Main scraper class with improved modularity."""

    def __init__(
        self,
        domain: str,
        max_retries: int = 3,
        max_concurrent_requests: int = 3,
        batch_size: int = 50,
        max_items: Optional[int] = None,
    ):
        self.logger = LoggingConfig.setup_logging()
        self.proxy_config = ProxyConfig(domain)
        self.domain = domain
        self.base_search_url = self._get_base_url()

        # Configuration

        self.max_retries = max_retries
        self.max_concurrent_requests = max_concurrent_requests
        self.batch_size = batch_size
        self.max_items = max_items

        # Constants

        self.page_load_timeout = 30
        self.retry_delay = 5
        self.retry_statuses = {408, 429, 500, 502, 503, 504}
        self.min_content_length = 50000

        # State tracking

        self.scraped_urls: Set[str] = set()
        self.products: List[Dict] = []
        self.product_buffer: Deque[Dict] = deque(maxlen=batch_size)
        self.total_saved = 0
        self.current_output_file = None
        self.last_saved_products = []
        self.semaphore = asyncio.Semaphore(max_concurrent_requests)
        self.items_scraped = 0
        self.save_count = 0
        self.backup_interval = 20
        self.saved_product_ids = set()

        self._setup_signal_handlers()

    def _get_base_url(self) -> str:
        """Get and validate base URL for the domain."""
        base_url = EBAY_DOMAIN.get(self.domain)
        if not base_url:
            raise ConfigurationError(
                f"Invalid domain identifier. Valid options are: {
                    ', '.join(EBAY_DOMAIN.keys())}"
            )
        return f"{base_url}/sch/i.html?"

    def _setup_signal_handlers(self):
        """Set up signal handlers for graceful shutdown."""
        try:
            for sig in (signal.SIGTERM, signal.SIGINT):
                signal.signal(sig, self._handle_shutdown)
        except AttributeError:
            pass

    def _handle_shutdown(self, signum, frame):
        """Handle shutdown signals."""
        self.logger.info("Shutdown signal received. Saving remaining data...")
        if self.product_buffer and self.current_output_file:
            loop = asyncio.get_event_loop()
            loop.run_until_complete(self.force_save_buffer())
        self.logger.info("Shutdown complete")
        sys.exit(0)

    @staticmethod
    def _clean_text(text: str) -> str:
        """Clean and normalize text content."""
        return " ".join(text.split()).strip()

    async def _make_request(
        self, session: AsyncSession, url: str, page_type: str
    ) -> Tuple[int, Optional[str]]:
        """Make HTTP request with retries and validation."""
        retries = 0
        last_error = None

        while retries < self.max_retries:
            try:
                async with self.semaphore:
                    response = await session.get(
                        url,
                        proxies=self.proxy_config.get_proxy_config(),
                        impersonate="chrome124",
                        timeout=self.page_load_timeout,
                    )

                    if response.status_code == 200:
                        content = response.text
                        is_valid, message = self._validate_page_content(
                            content, page_type
                        )

                        if is_valid:
                            return response.status_code, content
                        else:
                            self.logger.warning(
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
                        await asyncio.sleep(self.retry_delay * (retries + 1))
                        retries += 1
                        continue
                    else:
                        return response.status_code, None
            except Exception as e:
                last_error = str(e)
                self.logger.error(
                    f"Attempt {retries + 1} failed for {url}: {last_error}"
                )

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
        self.logger.error(
            f"All retry attempts failed for {
                          url}. Last error: {last_error}"
        )
        return None, None

    def _validate_page_content(
        self, html_content: str, page_type: str
    ) -> Tuple[bool, str]:
        """Validate page content for required elements."""
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
        """Extract seller/store information from the page."""
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

    async def _process_search_page(
        self, session: AsyncSession, page_num: int, search_term: str
    ) -> Tuple[bool, bool]:
        """
        Process a single search results page.

        Args:
            session: The async session to use for requests
            page_num: The page number to process
            search_term: The search term being processed

        Returns:
            Tuple[bool, bool]: (has_next_page, no_more_items)
        """
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
                self.logger.info(
                    f"Processing search page {page_num} (Attempt {retry_count + 1})"
                )

                status_code, html_content = await self._make_request(
                    session, url, "search_page"
                )

                if not html_content:
                    retry_count += 1
                    await asyncio.sleep(self.retry_delay * (retry_count + 1))
                    continue
                soup = BeautifulSoup(html_content, "lxml")
                product_urls = self._extract_product_urls(soup)

                if not product_urls:
                    self.logger.info(
                        f"No more products found for '{search_term}' after {self.items_scraped} items"
                    )
                    no_more_items = True
                    return False, True
                self.logger.info(
                    f"Found {len(product_urls)} products on page {page_num}"
                )

                # Process products in chunks

                chunk_size = 5
                for i in range(0, len(product_urls), chunk_size):
                    if self.max_items and self.items_scraped >= self.max_items:
                        self.logger.info(
                            f"Reached maximum items limit ({self.max_items})"
                        )
                        return False, False
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

                        self.logger.info(
                            f"Progress: {self.items_scraped}/"
                            f"{self.max_items if self.max_items else 'unlimited'} items"
                        )
                    await asyncio.sleep(1)
                has_next = self._has_next_page(soup)
                return (
                    has_next
                    and (not self.max_items or self.items_scraped < self.max_items),
                    False,
                )
            except Exception as e:
                self.logger.error(f"Error processing page {page_num}: {str(e)}")
                retry_count += 1
                await asyncio.sleep(self.retry_delay * (retry_count + 1))
        self.logger.error(f"Failed to process page {page_num} after all retries")
        return False, False

    def _has_next_page(self, soup: BeautifulSoup) -> bool:
        """Check if there is a next page of results."""
        next_link = soup.select_one('a[type="next"]')
        if next_link and next_link.get("href"):
            return True
        next_button = soup.select_one('button[type="next"]')
        if next_button and next_button.get("aria-disabled") == "true":
            return False
        return bool(next_link)

    def _extract_product_urls(self, soup: BeautifulSoup) -> List[str]:
        """Extract product URLs from the search results page."""
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

    def _extract_product_details(self, soup: BeautifulSoup, url: str) -> ProductDetails:
        """Extract all product details from the page."""
        details = ProductDetails(url=url)

        try:
            details.store_info = self._extract_store_info(soup)

            # Extract basic details

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
            # Extract shipping and location

            if shipping_div := soup.select_one("div.ux-labels-values--shipping"):
                if values_content := shipping_div.select_one(
                    "div.ux-labels-values__values-content"
                ):
                    details.shipping = self._extract_shipping_info(values_content)
                    details.location = self._extract_location_info(values_content)
            # Extract returns policy

            if returns_div := soup.select_one("div.x-returns-minview"):
                details.returns = self._extract_returns_info(returns_div)
            # Extract condition

            if condition_span := soup.select_one(
                "div.x-item-condition-max-view .ux-section__item > span.ux-textspans"
            ):
                details.condition = condition_span.text.strip().split(".")[0] + "."
            # Extract brand and type

            details.brand = self._extract_field_value(soup, "brand")
            details.type = self._extract_field_value(soup, "type")
        except Exception as e:
            self.logger.error(f"Error extracting details from {url}: {str(e)}")
        return details

    def _extract_shipping_info(self, values_content: BeautifulSoup) -> Optional[str]:
        """Extract shipping information from the content."""
        shipping_text_parts = []

        if first_div := values_content.find("div"):
            shipping_span = first_div.find(
                "span",
                class_=lambda x: x
                and all(
                    c in x for c in ["ux-textspans--POSITIVE", "ux-textspans--BOLD"]
                ),
            )

            if shipping_span:
                shipping_text_parts = [shipping_span.text.strip()]
            else:
                spans = first_div.find_all("span", class_="ux-textspans")
                for span in spans:
                    if (
                        span.text.strip()
                        and "clipped" not in span.get("class", [])
                        and "See details" not in span.text
                    ):

                        text = span.text.strip()
                        if text and text not in [".", "&nbsp;", " "]:
                            shipping_text_parts.append(text)
        return " ".join(shipping_text_parts).strip() if shipping_text_parts else None

    def _extract_location_info(self, values_content: BeautifulSoup) -> Optional[str]:
        """Extract location information from the content."""
        if location_span := values_content.find(
            "span", class_="ux-textspans--SECONDARY"
        ):
            location_text = location_span.text.strip()
            if location_text.startswith("Located in: "):
                return location_text.replace("Located in: ", "")
        return None

    def _extract_returns_info(self, returns_div: BeautifulSoup) -> Optional[str]:
        """Extract returns information from the content."""
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

            return returns_text
        return None

    def _extract_field_value(
        self, soup: BeautifulSoup, field_type: str
    ) -> Optional[str]:
        """Extract value for a specific field type (brand/type)."""
        if (field_dl := soup.select_one(f"dl.ux-labels-values--{field_type}")) and (
            field_value := field_dl.select_one("dd .ux-textspans")
        ):
            return field_value.text.strip()
        elif (field_div := soup.select_one(f"div.ux-labels-values--{field_type}")) and (
            field_value := field_div.select_one(
                ".ux-labels-values__values .ux-textspans"
            )
        ):
            return field_value.text.strip()
        return None

    async def save_batch(self, batch: List[Dict]):
        """Save a batch of products to file."""
        if not batch:
            return
        try:
            os.makedirs(os.path.dirname(self.current_output_file), exist_ok=True)

            self.save_count += 1
            if self.save_count % self.backup_interval == 0:
                await self._create_backup()
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
            updated_data = {"products": all_products}

            await self._save_to_file(temp_file, updated_data)
            os.replace(temp_file, self.current_output_file)

            self.total_saved = len(all_products)
            self.logger.info(
                f"Saved batch of {len(new_products)} new products. Total saved: {
                    self.total_saved}"
            )
        except Exception as e:
            self.logger.error(f"Error saving batch: {str(e)}")
            await self._create_emergency_backup(batch)


    async def _load_existing_data(self) -> Dict:
        """Load existing data from file."""
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
                self.logger.error("Error reading existing file, starting fresh")
        return existing_data


    async def _save_to_file(self, file_path: str, data: Dict):
        """Save data to file."""
        async with aiofiles.open(file_path, "w", encoding="utf-8") as f:
            await f.write(json.dumps(data, indent=2, ensure_ascii=False))


    async def _create_backup(self):
        """Create a backup of the current data file."""
        if os.path.exists(self.current_output_file):
            try:
                async with aiofiles.open(
                    self.current_output_file, "r", encoding="utf-8"
                ) as f:
                    current_content = await f.read()
                    current_data = json.loads(current_content)

                    if current_data.get("products") == self.last_saved_products:
                        self.logger.info("No significant changes detected, skipping backup")
                        return
                backup_file = f"{self.current_output_file}.backup"
                await self._save_to_file(backup_file, current_data)
                self.logger.info(f"Backup updated: {backup_file}")
                self.last_saved_products = current_data.get("products")
            except Exception as e:
                self.logger.error(f"Failed to create backup: {str(e)}")


    async def _create_emergency_backup(self, batch: List[Dict]):
        """Create an emergency backup in case of save failure."""
        try:
            emergency_backup = f"{self.current_output_file}.emergency_{
                    datetime.now().strftime('%Y%m%d_%H%M%S')}"
            emergency_data = {
                "products": batch,
                "total_products": len(batch),
                "requested_items": self.max_items,
                "actual_items_found": len(batch),
                "last_updated": datetime.now().isoformat(),
            }
            await self._save_to_file(emergency_backup, emergency_data)
            self.logger.info(f"Created emergency backup: {emergency_backup}")
        except Exception as backup_error:
            self.logger.error(f"Emergency backup failed: {str(backup_error)}")


    async def force_save_buffer(self):
        """Force save the current buffer contents."""
        if self.product_buffer and self.current_output_file:
            await self.save_batch(list(self.product_buffer))
            self.product_buffer.clear()


    async def add_to_buffer(self, product: Dict):
        """Add a product to the buffer and save if full."""
        self.product_buffer.append(product)
        if len(self.product_buffer) >= self.batch_size:
            await self.save_batch(list(self.product_buffer))
            self.product_buffer.clear()


    async def scrape_product(self, session: AsyncSession, url: str) -> Optional[Dict]:
        """Scrape a single product page."""
        try:
            status_code, html_content = await self._make_request(
                session, url, "product_page"
            )

            if not html_content:
                self.logger.error(f"Failed to load product page properly: {url}")
                return None
            soup = BeautifulSoup(html_content, "lxml")
            details = self._extract_product_details(soup, url)

            if not details.title or not details.current_price:
                self.logger.warning(f"Incomplete product data for {url}")
                return None
            self.scraped_urls.add(url)
            product_dict = asdict(details)
            await self.add_to_buffer(product_dict)

            self.logger.info(f"Successfully scraped: {details.title}")
            return product_dict
        except Exception as e:
            self.logger.error(f"Error scraping {url}: {str(e)}")
            return None


    async def scrape_all(self, search_terms: List[str], output_dir: str) -> Dict[str, Dict]:
        """Main method to scrape all products for given search terms."""
        all_results = {}

        for search_term in search_terms:
            self.items_scraped = 0
            self.products = []
            self.scraped_urls = set()
            self.product_buffer.clear()

            output_file = (
                Path(output_dir) / f"ebay_products_{self.domain}_{search_term}.json"
            )
            self.current_output_file = str(output_file).replace(" ", "_")

            self.logger.info(f"Starting scrape for search term: {search_term}")

            async with AsyncSession() as session:
                page_num = 1
                try:
                    while True:
                        has_next, no_more_items = await self._process_search_page(
                            session, page_num, search_term
                        )

                        if no_more_items or not has_next:
                            self._log_completion_status(search_term, no_more_items)
                            break
                        page_num += 1
                        await asyncio.sleep(1)
                    await self.force_save_buffer()
                    all_results[search_term] = self._create_result_summary()
                except Exception as e:
                    self.logger.error(
                        f"Error during scraping '{
                                    search_term}': {str(e)}"
                    )
                    await self.force_save_buffer()
                    all_results[search_term] = self._create_error_summary(str(e))
        return all_results


    def _log_completion_status(self, search_term: str, no_more_items: bool):
        """Log the completion status of scraping."""
        if no_more_items:
            self.logger.warning(
                f"Search term '{search_term}' has fewer products ({
                    self.items_scraped}) "
                f"than requested ({self.max_items})"
            )
        elif self.max_items and self.items_scraped < self.max_items:
            self.logger.warning(
                f"Could only find {self.items_scraped} items for '{
                    search_term}', "
                f"fewer than requested {self.max_items}"
            )
        else:
            self.logger.info(
                f"Completed scraping for '{search_term}' with {
                    self.items_scraped} items"
            )


    def _create_result_summary(self) -> Dict:
        """Create a summary of scraping results."""
        return {
            "items_found": self.items_scraped,
            "items_requested": self.max_items,
            "products": self.products.copy(),
        }


    def _create_error_summary(self, error: str) -> Dict:
        """Create a summary when scraping encounters an error."""
        return {
            "items_found": self.items_scraped,
            "items_requested": self.max_items,
            "products": self.products.copy(),
            "error": error,
        }

async def main():
    """Main entry point of the scraper."""
    try:
        os.makedirs("output", exist_ok=True)

        domain = "AU"
        print(f"Scraping eBay domain: 🌐 {domain} - {EBAY_DOMAIN[domain]}")

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
