# Universal Product Extractor for E-Commerce Result Pages
"""
Universal extractor that parses product listings from arbitrary e-commerce
search/result pages using layered, comprehensive selector strategies and
robust fallbacks (including schema.org JSON-LD parsing).

Design mirrors `LaunchPad/universalSearch.py` for consistency:
- Extensive CSS/XPath selector families
- Selenium-based DOM discovery with smart waits
- Normalization utilities (price parsing, URL resolution, text cleanup)
- Optional JSON-LD/schema.org extraction as a fallback
"""

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException
try:
    from webdriver_manager.chrome import ChromeDriverManager
    WEBDRIVER_MANAGER_AVAILABLE = True
except ImportError:
    WEBDRIVER_MANAGER_AVAILABLE = False
from urllib.parse import urlparse, urljoin
import json
import re
import time
import os
from typing import List, Dict, Any, Optional

# Supabase imports
try:
    from supabase import create_client, Client
    SUPABASE_AVAILABLE = True
except ImportError:
    SUPABASE_AVAILABLE = False
    print("[!] Warning: supabase-py not installed. Install with: pip install supabase")

# Supabase configuration
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://whfjofihihlhctizchmj.supabase.co")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6IndoZmpvZmloaWhsaGN0aXpjaG1qIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjEzNzQzNDMsImV4cCI6MjA3Njk1MDM0M30.OsJnOqeJgT5REPg7uxkGmmVcHIcs5QO4vdyDi66qpR0")


class UniversalProductExtractor:
    """
    Extract product data (title, price, image, link, availability, ratings, etc.)
    from any e-commerce listing/search page using layered strategies.
    """

    def __init__(self):
        self.selector_sets = self._build_selector_sets()
        
        # Initialize Supabase connection
        self.supabase: Optional[Client] = None
        if SUPABASE_AVAILABLE and SUPABASE_KEY:
            try:
                self.supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
                print("[✓] Connected to Supabase for product storage\n")
            except Exception as e:
                print(f"[!] Warning: Failed to connect to Supabase: {e}")
                print("[!] Products will be extracted but not saved to database\n")
                self.supabase = None
        elif not SUPABASE_AVAILABLE:
            print("[!] Warning: supabase-py not installed. Products will not be saved to database\n")
        elif not SUPABASE_KEY:
            print("[!] Warning: SUPABASE_KEY not set. Products will not be saved to database\n")

        # Heuristic phrases and keywords used across strategies
        self.no_results_phrases = [
            'no results',
            'no results found',
            'no result found',
            '0 results',
            '0 result',
            'no product',
            'nothing found',
            'did not find anything',
            'did not find anythings',
            'we did not find',
            'we did not find anything',
            'we did not find anythings',
            'try another search',
            'try a different search',
        ]

        self.link_blacklist_keywords = [
            'login', 'register', 'signup', 'account', 'profile', 'help', 'faq', 'contact',
            'privacy', 'terms', 'policy', 'cart', 'wishlist', 'checkout', 'track', 'order',
            'facebook', 'instagram', 'whatsapp', 'twitter', 'youtube', 'pinterest',
            'linkedin', 'support', 'mailto:', 'tel:', 'javascript:', 'gift-card', 'loyalty',
        ]

        self.product_path_keywords = [
            '/product', '/products', '/item', '/items', '/p/', '/dp/', '/pd/', '/pdp',
            '/shop/', '/store/', '/catalog', '/listing', '/sku', '/detail', '/details',
            '/gp/', '/gp/product', '/listing/', '/prod', '/itm', '/itm/',
            'collection', 'collections', 'category', 'categories',
            'productId', 'sku=', 'pid=', 'variant=', 'model=', '/buy/', '/sale/',
        ]

        self.blacklisted_sections = {'header', 'nav', 'footer', 'aside', 'form'}

        self.load_more_selectors = [
            'button[class*="load" i]',
            'button[id*="load" i]',
            'button[data-test*="load" i]',
            'button[data-testid*="load" i]',
            'button[aria-label*="load" i]',
            'button[class*="more" i]',
            'a[class*="load" i]',
            'div[class*="load-more" i]',
            '[data-action*="loadMore" i]',
        ]

        self.popup_close_selectors = [
            'button[aria-label*="close" i]',
            'button[class*="close" i]',
            'button[class*="dismiss" i]',
            '[role="dialog"] button',
            '.close-button',
            '.modal-close',
            '.overlay-close',
            '[data-testid*="close" i]',
            '[data-action*="close" i]',
            '[aria-label*="dismiss" i]',
        ]

        self.max_scroll_attempts = 4

    def _build_selector_sets(self) -> Dict[str, List[str]]:
        """Define comprehensive selectors for product cards and fields."""
        return {
            # Common result container scopes (helps avoid grabbing banners/footers)
            "result_containers": [
                'ul.products',
                'ul.product-list',
                'ul.search-results',
                'div.products',
                'div.product-list',
                'div.search-results',
                'div[class*="listing" i]',
                'div[class*="product-grid" i]',
                'div[data-component*="product" i]',
                'div[data-testid*="result" i]',
                'section[class*="grid" i]',
                'section[class*="listing" i]',
                'section[class*="catalog" i]',
                'div[class*="grid" i]',
                'section[class*="product" i]',
                'section[class*="result" i]',
                'main',
            ],
            # Product card/container candidates
            "product_cards": [
                '[data-component="product"]',
                '[data-qa*="product" i]',
                '[data-testid*="product" i]',
                '[data-cy*="product" i]',
                '[itemscope][itemtype*="schema.org/Product" i]',
                'div[data-product-id]',
                'article[data-product-id]',
                'div[data-asin]',
                'li[data-asin]',
                'li[data-id*="product" i]',
                'div[data-testid*="product-card" i]',
                'li[class*="product" i]',
                'li[class*="grid" i]',
                'div[class*="product" i]',
                'div[class*="item" i]',
                'div[class*="card" i]',
                'div[class*="result" i]',
                'article[class*="product" i]',
                'article[class*="item" i]',
            ],

            # Title within a card
            "title": [
                '[itemprop="name"]',
                'a[title]',
                'a[class*="title" i]',
                'a[data-testid*="title" i]',
                'h1', 'h2', 'h3', 'h4',
                '[class*="title" i]',
                '[class*="name" i]',
                '[aria-label*="product" i]',
            ],

            # Link within a card
            "link": [
                'a[href*="/product" i]',
                'a[href*="/item" i]',
                'a[href*="/p/" i]',
                'a[href*="?pid=" i]',
                'a[data-testid*="product" i]',
                'a[data-track*="product" i]',
                'a[href]',
                '[itemprop="url"]',
            ],

            # Image within a card
            "image": [
                'img[src]',
                'img[data-src]',
                'img[data-original]',
                'img[data-lazy-src]',
                'img[data-srcset]',
                'source[data-srcset]',
                '[data-background-image]',
                '[itemprop="image"]',
            ],

            # Price within a card
            "price": [
                '[itemprop="price"]',
                '[class*="price" i]',
                '[class*="offer" i]',
                '[data-price]',
                'span[data-price]',
                'div[data-price]',
                'span[class*="amount" i]',
                'span[class*="value" i]',
                'meta[itemprop="price"][content]',
            ],

            # Currency within a card
            "currency": [
                'meta[itemprop="priceCurrency"][content]',
                '[class*="currency" i]',
                'span[data-currency]',
            ],

            # Rating within a card
            "rating": [
                '[itemprop="ratingValue"]',
                '[class*="rating" i]',
                '[aria-label*="rating" i]',
            ],

            # Reviews count
            "reviews": [
                '[itemprop="reviewCount"]',
                '[class*="review" i]',
                '[aria-label*="review" i]',
            ],

            # Availability
            "availability": [
                '[itemprop="availability"]',
                '[class*="stock" i]',
                '[class*="avail" i]',
            ],

            # Brand
            "brand": [
                '[itemprop="brand"]',
                '[class*="brand" i]',
                '[data-brand]',
            ],

            # SKU / product code
            "sku": [
                '[itemprop="sku"]',
                '[data-sku]',
                '[data-product-sku]',
                '[class*="sku" i]',
            ],

            # Description snippet
            "description": [
                '[itemprop="description"]',
                '[class*="description" i]',
                '[class*="subtitle" i]',
                'p',
            ],
        }

    def extract_products(self, url: str, max_items: int = 50, wait_seconds: int = 12, 
                         category_id: Optional[int] = None, searched_product_id: Optional[int] = None) -> Dict[str, Any]:
        """
        Load the page and extract a list of products.

        Args:
            url: URL to extract products from
            max_items: Maximum number of products to extract
            wait_seconds: Wait time for page load
            category_id: ID of the category this search belongs to (optional)
            searched_product_id: ID of the product from products table that was searched for (optional)

        Returns a dict containing metadata and an array of product dicts.
        """
        driver = None
        try:
            driver = self._setup_driver()
            print(f"[Universal Extractor] Navigating: {url}")
            driver.get(url)

            # Wait for the DOM to be ready
            WebDriverWait(driver, wait_seconds).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )

            # Handle popups/load-more/infinite scroll before extraction
            self._dismiss_known_popups(driver)
            self._progressive_scroll_and_load(driver)

            # Try to wait for any of the product card selectors after prep
            self._wait_for_any_selector(driver, self.selector_sets["product_cards"], wait_seconds)

            # Strategy 1: DOM-based extraction within scoped containers
            products = self._extract_from_dom(driver, url, max_items)

            # If nothing found, try JSON-LD fallback
            if not products:
                products = self._extract_from_jsonld(driver, url, max_items)

            # Structured data via microdata (itemscope/itemprop)
            if len(products) == 0:
                products = self._extract_from_microdata(driver, url, max_items)

            # Inline JSON data structures (application/json scripts)
            if len(products) == 0:
                products = self._extract_from_inline_data_scripts(driver, url, max_items)

            # Strategy 2: Heuristic global scan if still weak results
            if len(products) == 0:
                products = self._extract_by_global_heuristics(driver, url, max_items)

            # Strategy 3: Last resort - anchors that look like products (image + product-like path)
            if len(products) == 0:
                products = self._extract_from_links_with_images(driver, url, max_items)

            # If still nothing and page clearly indicates "no results", return empty
            if not products and self._page_indicates_no_results(driver):
                return {
                    "success": True,
                    "page_url": url,
                    "platform": urlparse(url).netloc,
                    "num_products": 0,
                    "products": [],
                }

            # Deduplicate by product_url
            products = self._dedupe_by_url(products)
            if len(products) > max_items:
                products = products[:max_items]

            # Get platform URL from the extracted URL
            platform_url = url
            platform = urlparse(url).netloc

            # Save products to database (with category and searched product info)
            saved_count = self._save_products_to_db(
                products, 
                platform_url, 
                platform,
                category_id=category_id,
                searched_product_id=searched_product_id
            )

            return {
                "success": True,
                "page_url": url,
                "platform": platform,
                "num_products": len(products),
                "products": products,
                "saved_to_db": saved_count,
            }

        except Exception as e:
            return {"success": False, "page_url": url, "error": str(e)}
        finally:
            if driver:
                driver.quit()

    # ----------------------------- DOM Extraction -----------------------------

    def _extract_from_dom(self, driver: webdriver.Chrome, base_url: str, max_items: int) -> List[Dict[str, Any]]:
        products: List[Dict[str, Any]] = []

        # Scope search to likely result containers first
        container_elements = self._find_first_nonempty_set(
            driver, self.selector_sets.get("result_containers", []), By.CSS_SELECTOR
        )

        card_elements = []
        if container_elements:
            for cont in container_elements:
                try:
                    for sel in self.selector_sets["product_cards"]:
                        els = cont.find_elements(By.CSS_SELECTOR, sel)
                        card_elements.extend([e for e in els if e.is_displayed()])
                except Exception:
                    continue
        else:
            card_elements = self._find_first_nonempty_set(
                driver, self.selector_sets["product_cards"], By.CSS_SELECTOR
            )

        # If no obvious cards, try a permissive guess: any li/div with link+image
        if not card_elements:
            candidates = driver.find_elements(By.CSS_SELECTOR, "li, div, article")
            card_elements = [el for el in candidates if self._looks_like_product_card(el)]

        accepted = 0
        for card in card_elements:
            try:
                if self._is_within_blacklisted_section(card):
                    continue
                product = self._extract_fields_from_card(card, base_url)
                if product and self._is_valid_product(product, base_url):
                    products.append(product)
                    accepted += 1
                    if accepted >= max_items:
                        break
            except Exception:
                continue

        return products

    def _looks_like_product_card(self, el) -> bool:
        try:
            has_link = False
            has_image = False
            try:
                el.find_element(By.CSS_SELECTOR, "a[href]")
                has_link = True
            except Exception:
                pass
            try:
                el.find_element(By.CSS_SELECTOR, "img[src], img[data-src], img[data-original]")
                has_image = True
            except Exception:
                pass
            text = (el.text or "").lower()
            priceish = any(tok in text for tok in ["$", "₹", "rs.", "rs ", "usd", "eur", "price"])  # quick heuristic
            return has_link and (has_image or priceish)
        except Exception:
            return False

    def _extract_fields_from_card(self, card, base_url: str) -> Dict[str, Any]:
        def find_text(selectors: List[str]) -> Optional[str]:
            for sel in selectors:
                try:
                    el = card.find_element(By.CSS_SELECTOR, sel)
                    txt = el.get_attribute("content") or el.get_attribute("aria-label") or el.text
                    txt = self._clean_text(txt)
                    if txt:
                        return txt
                except Exception:
                    continue
            return None

        def find_attr(selectors: List[str], attr: str) -> Optional[str]:
            for sel in selectors:
                try:
                    el = card.find_element(By.CSS_SELECTOR, sel)
                    val = el.get_attribute(attr)
                    if val:
                        return val
                except Exception:
                    continue
            return None

        # Prefer link text as title if available
        title = None
        try:
            a = card.find_element(By.CSS_SELECTOR, 'a[href]')
            title = self._clean_text(a.get_attribute('title') or a.text)
        except Exception:
            pass
        # Fallback to image alt if still missing
        if not title:
            try:
                img = card.find_element(By.CSS_SELECTOR, 'img')
                title = self._clean_text(img.get_attribute('alt'))
            except Exception:
                pass
        if not title:
            title = find_text(self.selector_sets["title"]) or None

        # Prefer link from the most specific selector order
        link_href = None
        for sel in self.selector_sets["link"]:
            try:
                el = card.find_element(By.CSS_SELECTOR, sel)
                href = el.get_attribute("href") or el.get_attribute("content")
                if href:
                    link_href = href
                    break
            except Exception:
                continue

        image_src = None
        for sel in self.selector_sets["image"]:
            try:
                el = card.find_element(By.CSS_SELECTOR, sel)
                image_src = (
                    el.get_attribute("src")
                    or el.get_attribute("data-src")
                    or el.get_attribute("data-original")
                    or el.get_attribute("data-srcset")
                    or el.get_attribute("content")
                )
                if image_src:
                    break
            except Exception:
                continue

        # Price and currency
        raw_price = None
        for sel in self.selector_sets["price"]:
            try:
                el = card.find_element(By.CSS_SELECTOR, sel)
                raw_price = el.get_attribute("content") or el.text
                raw_price = self._clean_text(raw_price)
                if raw_price:
                    break
            except Exception:
                continue

        currency = None
        for sel in self.selector_sets["currency"]:
            try:
                el = card.find_element(By.CSS_SELECTOR, sel)
                currency = el.get_attribute("content") or el.text
                currency = self._clean_text(currency)
                if currency:
                    break
            except Exception:
                continue

        # Try parsing price from entire card text if selector missed
        if not raw_price:
            try:
                raw_price = self._extract_price_from_text(card.text)
            except Exception:
                pass

        parsed_price, detected_currency = self._parse_price(raw_price)
        if not currency:
            currency = detected_currency

        # Ratings and reviews (best-effort heuristics)
        rating_text = find_text(self.selector_sets["rating"]) or None
        review_text = find_text(self.selector_sets["reviews"]) or None
        rating_value = self._parse_rating(rating_text)
        review_count = self._parse_int(review_text)

        # Availability
        availability_text = find_text(self.selector_sets["availability"]) or None
        in_stock = self._infer_in_stock(availability_text)

        # Brand / SKU / Description
        brand = find_text(self.selector_sets["brand"]) or find_attr(self.selector_sets["brand"], "data-brand")

        sku = find_text(self.selector_sets["sku"]) or find_attr(self.selector_sets["sku"], "data-sku")
        if not sku:
            sku = find_attr(self.selector_sets["sku"], "data-product-sku")

        description = None
        for sel in self.selector_sets["description"]:
            try:
                el = card.find_element(By.CSS_SELECTOR, sel)
                desc = el.get_attribute("content") or el.text
                desc = self._clean_text(desc)
                if desc and len(desc) > 15:
                    description = desc[:400]
                    break
            except Exception:
                continue

        return {
            "title": title,
            "product_url": self._to_absolute(base_url, link_href) if link_href else None,
            "image_url": self._to_absolute(base_url, image_src) if image_src else None,
            "price": parsed_price,
            "currency": currency,
            "raw_price": raw_price,
            "rating": rating_value,
            "review_count": review_count,
            "in_stock": in_stock,
            "brand": brand,
            "sku": sku,
            "description": description,
        }

    # ----------------------------- JSON-LD Fallback ----------------------------

    def _extract_from_jsonld(self, driver: webdriver.Chrome, base_url: str, max_items: int) -> List[Dict[str, Any]]:
        products: List[Dict[str, Any]] = []
        scripts = driver.find_elements(By.XPATH, "//script[@type='application/ld+json']")
        for s in scripts:
            try:
                content = s.get_attribute("innerText") or ""
                blobs = self._safe_jsons_from_script(content)
                for blob in blobs:
                    self._collect_products_from_ldjson(blob, base_url, products, max_items)
            except Exception:
                continue
        return products[:max_items]

    def _collect_products_from_ldjson(self, data: Any, base_url: str, out: List[Dict[str, Any]], max_items: int):
        if len(out) >= max_items:
            return
        try:
            if isinstance(data, list):
                for item in data:
                    self._collect_products_from_ldjson(item, base_url, out, max_items)
            elif isinstance(data, dict):
                t = (data.get("@type") or data.get("type") or "").lower()
                if t in ["product", "listitem"] or "Product" in str(data.get("@type")):
                    product = self._map_ldjson_product(data, base_url)
                    if product and self._is_valid_product(product, base_url):
                        out.append(product)
                # Sometimes data is under itemListElement
                if "itemListElement" in data:
                    self._collect_products_from_ldjson(data["itemListElement"], base_url, out, max_items)
                if "mainEntity" in data:
                    self._collect_products_from_ldjson(data["mainEntity"], base_url, out, max_items)
        except Exception:
            return

    def _map_ldjson_product(self, d: Dict[str, Any], base_url: str) -> Optional[Dict[str, Any]]:
        name = d.get("name") or (d.get("item") or {}).get("name")
        url = d.get("url") or (d.get("item") or {}).get("url")
        image = d.get("image")
        if isinstance(image, list) and image:
            image = image[0]
        offers = d.get("offers") or {}
        if isinstance(offers, list) and offers:
            offers = offers[0]
        price = offers.get("price") if isinstance(offers, dict) else None
        currency = offers.get("priceCurrency") if isinstance(offers, dict) else None
        availability = offers.get("availability") if isinstance(offers, dict) else None
        agg_rating = d.get("aggregateRating") or {}
        rating_value = agg_rating.get("ratingValue") if isinstance(agg_rating, dict) else None
        review_count = agg_rating.get("reviewCount") if isinstance(agg_rating, dict) else None

        brand = d.get("brand")
        if isinstance(brand, dict):
            brand = brand.get("name") or brand.get("brand")
        elif isinstance(brand, list) and brand:
            first_brand = brand[0]
            if isinstance(first_brand, dict):
                brand = first_brand.get("name") or first_brand.get("brand")
            else:
                brand = first_brand

        sku = d.get("sku") or (d.get("item") or {}).get("sku")
        description = d.get("description") or (d.get("item") or {}).get("description")

        parsed_price, detected_currency = self._parse_price(str(price) if price is not None else None)
        if not currency:
            currency = detected_currency

        return {
            "title": self._clean_text(name),
            "product_url": self._to_absolute(base_url, url) if url else None,
            "image_url": self._to_absolute(base_url, image) if isinstance(image, str) else None,
            "price": parsed_price,
            "currency": currency,
            "raw_price": str(price) if price is not None else None,
            "rating": self._parse_float(rating_value),
            "review_count": self._parse_int(review_count),
            "in_stock": self._infer_in_stock(availability),
            "brand": self._clean_text(brand),
            "sku": self._clean_text(sku),
            "description": self._clean_text(description),
        }

    def _safe_jsons_from_script(self, content: str) -> List[Any]:
        blobs: List[Any] = []
        try:
            # Some sites embed multiple JSON objects or arrays; try naive splits
            candidates = [content]
            # Extract JSON-like blocks using braces/brackets balance heuristics
            # Fallback to raw parse if single block
            for cand in candidates:
                try:
                    parsed = json.loads(cand)
                    blobs.append(parsed)
                except Exception:
                    # Try to salvage arrays/objects inside
                    for match in re.findall(r"(\{.*?\}|\[.*?\])", cand, flags=re.DOTALL):
                        try:
                            blobs.append(json.loads(match))
                        except Exception:
                            continue
        except Exception:
            pass
        return blobs

    # ------------------------------- Utilities --------------------------------

    def _dismiss_known_popups(self, driver: webdriver.Chrome):
        for selector in self.popup_close_selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                for element in elements[:2]:
                    try:
                        if element.is_displayed():
                            driver.execute_script("arguments[0].click();", element)
                            time.sleep(0.3)
                    except Exception:
                        continue
            except Exception:
                continue

    def _progressive_scroll_and_load(self, driver: webdriver.Chrome):
        try:
            last_height = driver.execute_script("return document.body.scrollHeight")
        except Exception:
            last_height = 0

        for attempt in range(self.max_scroll_attempts):
            try:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            except Exception:
                break
            time.sleep(1.2)
            self._click_load_more(driver)
            self._dismiss_known_popups(driver)
            try:
                new_height = driver.execute_script("return document.body.scrollHeight")
            except Exception:
                break
            if new_height <= last_height:
                break
            last_height = new_height

    def _click_load_more(self, driver: webdriver.Chrome) -> bool:
        clicked = False
        for selector in self.load_more_selectors:
            try:
                buttons = driver.find_elements(By.CSS_SELECTOR, selector)
                for btn in buttons[:2]:
                    if btn.is_displayed() and btn.is_enabled():
                        try:
                            driver.execute_script("arguments[0].click();", btn)
                            time.sleep(0.5)  # Reduced from 1 to 0.5 seconds
                            clicked = True
                        except Exception:
                            continue
            except Exception:
                continue
        return clicked

    def _setup_driver(self) -> webdriver.Chrome:
        chrome_options = Options()
        # Headless mode optimized for Railway
        chrome_options.add_argument('--headless=new')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        
        # GPU and rendering optimizations for headless server
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--disable-software-rasterizer')
        chrome_options.add_argument('--disable-accelerated-2d-canvas')
        chrome_options.add_argument('--disable-accelerated-video-decode')
        chrome_options.add_argument('--disable-background-networking')
        chrome_options.add_argument('--disable-background-timer-throttling')
        chrome_options.add_argument('--disable-backgrounding-occluded-windows')
        chrome_options.add_argument('--disable-breakpad')
        chrome_options.add_argument('--disable-client-side-phishing-detection')
        chrome_options.add_argument('--disable-component-extensions-with-background-pages')
        chrome_options.add_argument('--disable-features=TranslateUI,BlinkGenPropertyTrees')
        chrome_options.add_argument('--disable-hang-monitor')
        chrome_options.add_argument('--disable-ipc-flooding-protection')
        chrome_options.add_argument('--disable-popup-blocking')
        chrome_options.add_argument('--disable-prompt-on-repost')
        chrome_options.add_argument('--disable-renderer-backgrounding')
        chrome_options.add_argument('--disable-sync')
        chrome_options.add_argument('--disable-web-resources')
        chrome_options.add_argument('--metrics-recording-only')
        chrome_options.add_argument('--no-first-run')
        chrome_options.add_argument('--safebrowsing-disable-auto-update')
        chrome_options.add_argument('--enable-automation')
        chrome_options.add_argument('--password-store=basic')
        chrome_options.add_argument('--use-mock-keychain')
        
        # Performance optimizations
        chrome_options.add_argument('--disable-extensions')
        chrome_options.add_argument('--disable-logging')
        chrome_options.add_argument('--disable-notifications')
        chrome_options.add_argument('--disable-default-apps')
        chrome_options.add_argument('--disable-plugins')
        chrome_options.add_argument('--disable-images')  # Disable images for faster loading
        chrome_options.add_argument('--blink-settings=imagesEnabled=false')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_argument('--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        
        # Disable unnecessary features
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"]) 
        chrome_options.add_experimental_option('useAutomationExtension', False)
        chrome_options.add_experimental_option("prefs", {
            "profile.default_content_setting_values": {
                "images": 2,  # Disable images
                "plugins": 2,  # Disable plugins
                "popups": 2,   # Block popups
                "geolocation": 2,  # Block geolocation
                "notifications": 2,  # Block notifications
            }
        })
        
        # Use webdriver-manager if available, otherwise try system Chrome
        if WEBDRIVER_MANAGER_AVAILABLE:
            try:
                service = Service(ChromeDriverManager().install())
                driver = webdriver.Chrome(service=service, options=chrome_options)
            except Exception as e:
                print(f"[!] WebDriver Manager failed, trying system Chrome: {e}")
                driver = webdriver.Chrome(options=chrome_options)
        else:
            driver = webdriver.Chrome(options=chrome_options)
        
        driver.set_page_load_timeout(30)
        return driver

    def _wait_for_any_selector(self, driver: webdriver.Chrome, selectors: List[str], wait_seconds: int):
        end = time.time() + wait_seconds
        while time.time() < end:
            for sel in selectors:
                try:
                    els = driver.find_elements(By.CSS_SELECTOR, sel)
                    visible = [e for e in els if e.is_displayed()]
                    if visible:
                        return
                except Exception:
                    continue
            time.sleep(0.25)
        # Soft timeout only; DOM extraction still attempts heuristics

    def _find_first_nonempty_set(self, driver: webdriver.Chrome, selectors: List[str], by: By):
        for sel in selectors:
            try:
                els = driver.find_elements(by, sel)
                els = [e for e in els if e.is_displayed()]
                if els:
                    return els
            except Exception:
                continue
        return []

    def _page_indicates_no_results(self, driver: webdriver.Chrome) -> bool:
        try:
            body_text = (driver.find_element(By.TAG_NAME, 'body').text or '').lower()
            return any(p in body_text for p in self.no_results_phrases)
        except Exception:
            return False

    # ------------------------ Structured Data Strategies -----------------------

    def _extract_from_microdata(self, driver: webdriver.Chrome, base_url: str, max_items: int) -> List[Dict[str, Any]]:
        products: List[Dict[str, Any]] = []
        try:
            nodes = driver.find_elements(By.CSS_SELECTOR, '[itemscope][itemtype*="Product" i]')
        except Exception:
            nodes = []

        for node in nodes:
            try:
                if self._is_within_blacklisted_section(node):
                    continue
            except Exception:
                pass
            try:
                product = self._extract_microdata_node(node, base_url)
                if product and self._is_valid_product(product, base_url):
                    products.append(product)
                    if len(products) >= max_items:
                        break
            except Exception:
                continue

        return products

    def _extract_microdata_node(self, node, base_url: str) -> Optional[Dict[str, Any]]:
        data: Dict[str, Any] = {}

        # Direct attributes on the node
        try:
            itemid = node.get_attribute('itemid')
            if itemid and not data.get('product_url'):
                data['product_url'] = self._to_absolute(base_url, itemid)
        except Exception:
            pass

        props = []
        try:
            props = node.find_elements(By.CSS_SELECTOR, '[itemprop]')
        except Exception:
            props = []

        for prop in props:
            try:
                key = prop.get_attribute('itemprop')
                if not key:
                    continue
                key = key.lower()
                value = (
                    prop.get_attribute('content')
                    or prop.get_attribute('href')
                    or prop.get_attribute('src')
                    or prop.text
                )
                value = self._clean_text(value)

                # Nested brand/item scopes
                if key == 'brand' and (not value or len(value) <= 2):
                    try:
                        nested_name = prop.find_element(By.CSS_SELECTOR, '[itemprop="name"]')
                        value = self._clean_text(
                            nested_name.get_attribute('content') or nested_name.text
                        )
                    except Exception:
                        pass

                if key == 'name' and value and not data.get('title'):
                    data['title'] = value
                elif key in ('url', 'link') and value and not data.get('product_url'):
                    data['product_url'] = self._to_absolute(base_url, value)
                elif key == 'image' and value and not data.get('image_url'):
                    data['image_url'] = self._to_absolute(base_url, value)
                elif key == 'price':
                    data['raw_price'] = value
                elif key in ('pricecurrency', 'currency') and value:
                    data['currency'] = value
                elif key == 'availability' and value:
                    data['availability'] = value
                elif key == 'description' and value:
                    data['description'] = value[:400]
                elif key == 'brand' and value and not data.get('brand'):
                    data['brand'] = value
                elif key == 'sku' and value and not data.get('sku'):
                    data['sku'] = value
                elif key == 'ratingvalue' and value:
                    data['rating'] = value
                elif key in ('reviewcount', 'ratingcount') and value:
                    data['review_count'] = value
            except Exception:
                continue

        parsed_price, detected_currency = self._parse_price(data.get('raw_price'))
        currency = data.get('currency') or detected_currency

        return {
            'title': self._clean_text(data.get('title')),
            'product_url': data.get('product_url'),
            'image_url': data.get('image_url'),
            'price': parsed_price,
            'currency': self._clean_text(currency),
            'raw_price': data.get('raw_price'),
            'rating': self._parse_float(data.get('rating')),
            'review_count': self._parse_int(data.get('review_count')),
            'in_stock': self._infer_in_stock(data.get('availability')),
            'brand': self._clean_text(data.get('brand')),
            'sku': self._clean_text(data.get('sku')),
            'description': self._clean_text(data.get('description')),
        }

    def _extract_from_inline_data_scripts(self, driver: webdriver.Chrome, base_url: str, max_items: int) -> List[Dict[str, Any]]:
        products: List[Dict[str, Any]] = []
        scripts = driver.find_elements(By.XPATH, "//script[@type='application/json' or @type='text/json' or @type='text/plain']")
        for s in scripts:
            try:
                raw = s.get_attribute('innerText') or ''
                if not raw:
                    continue
                if len(raw) > 500_000:
                    continue  # avoid huge blobs
                blobs = self._safe_jsons_from_script(raw)
                for blob in blobs:
                    self._collect_products_from_generic_json(blob, base_url, products, max_items)
                    if len(products) >= max_items:
                        return products
            except Exception:
                continue
        return products

    def _collect_products_from_generic_json(self, data: Any, base_url: str, out: List[Dict[str, Any]], max_items: int, depth: int = 0):
        if len(out) >= max_items or depth > 6:
            return
        try:
            if isinstance(data, list):
                for item in data:
                    self._collect_products_from_generic_json(item, base_url, out, max_items, depth + 1)
                    if len(out) >= max_items:
                        break
            elif isinstance(data, dict):
                product = self._map_generic_json_product(data, base_url)
                if product and self._is_valid_product(product, base_url):
                    out.append(product)
                    if len(out) >= max_items:
                        return

                for key, value in data.items():
                    if isinstance(value, (list, dict)):
                        key_lower = str(key).lower()
                        if any(k in key_lower for k in ['product', 'item', 'sku', 'listing', 'result', 'entries', 'records']):
                            self._collect_products_from_generic_json(value, base_url, out, max_items, depth + 1)
                        elif depth <= 1:
                            # Explore shallow keys even if they don't look product-like
                            self._collect_products_from_generic_json(value, base_url, out, max_items, depth + 1)
        except Exception:
            return

    def _map_generic_json_product(self, data: Dict[str, Any], base_url: str) -> Optional[Dict[str, Any]]:
        if not isinstance(data, dict):
            return None

        def extract_first(keys: List[str]):
            for key in keys:
                if key in data and data[key] not in (None, ''):
                    val = data[key]
                    if isinstance(val, list):
                        return val[0]
                    return val
            return None

        title = extract_first(['name', 'title', 'productName', 'product_name', 'label'])
        url = extract_first(['url', 'link', 'productUrl', 'productURL', 'href', 'canonicalUrl'])
        image = extract_first(['image', 'imageUrl', 'imageURL', 'thumbnail', 'thumbnailUrl', 'mediaUrl', 'picture'])
        raw_price = extract_first(['price', 'salePrice', 'offerPrice', 'priceValue', 'price_amount', 'priceWithTax'])
        currency = extract_first(['currency', 'currencyCode', 'priceCurrency'])
        brand = extract_first(['brand', 'manufacturer', 'maker'])
        sku = extract_first(['sku', 'id', 'productId', 'product_id', 'itemId'])
        description = extract_first(['description', 'shortDescription', 'summary'])
        rating = extract_first(['rating', 'ratingValue', 'averageRating', 'reviewRating'])
        review_count = extract_first(['reviewCount', 'reviewsCount', 'numberOfReviews', 'ratingCount'])
        availability = extract_first(['availability', 'stockStatus', 'availabilityStatus'])

        # Nested price dicts
        if isinstance(raw_price, dict):
            raw_price = raw_price.get('value') or raw_price.get('amount') or raw_price.get('price')

        if isinstance(url, dict):
            url = url.get('url') or url.get('href')

        if isinstance(image, dict):
            image = image.get('url') or image.get('src')

        parsed_price, detected_currency = self._parse_price(str(raw_price) if raw_price is not None else None)
        if not currency:
            currency = detected_currency

        product = {
            'title': self._clean_text(title),
            'product_url': self._to_absolute(base_url, url) if url else None,
            'image_url': self._to_absolute(base_url, image) if isinstance(image, str) else None,
            'price': parsed_price,
            'currency': self._clean_text(currency),
            'raw_price': str(raw_price) if raw_price is not None else None,
            'rating': self._parse_float(rating),
            'review_count': self._parse_int(review_count),
            'in_stock': self._infer_in_stock(availability),
            'brand': self._clean_text(brand),
            'sku': self._clean_text(sku),
            'description': self._clean_text(description),
        }

        if not product.get('title') and not product.get('product_url'):
            return None
        return product

    # -------------------------- Additional Strategies --------------------------

    def _extract_by_global_heuristics(self, driver: webdriver.Chrome, base_url: str, max_items: int) -> List[Dict[str, Any]]:
        products: List[Dict[str, Any]] = []
        # Avoid header/footer/nav/aside
        candidates = driver.find_elements(By.CSS_SELECTOR, "main, section, div")
        candidates = [c for c in candidates if c.is_displayed()]
        for cont in candidates:
            try:
                if self._is_within_blacklisted_section(cont):
                    continue
                cards = cont.find_elements(By.CSS_SELECTOR, 'li, div, article')
                for card in cards:
                    if not card.is_displayed():
                        continue
                    if self._is_within_blacklisted_section(card):
                        continue
                    if not self._looks_like_product_card(card):
                        continue
                    product = self._extract_fields_from_card(card, base_url)
                    if product and self._is_valid_product(product, base_url):
                        products.append(product)
                        if len(products) >= max_items:
                            return products
            except Exception:
                continue
        return products

    def _extract_from_links_with_images(self, driver: webdriver.Chrome, base_url: str, max_items: int) -> List[Dict[str, Any]]:
        products: List[Dict[str, Any]] = []
        anchors = driver.find_elements(By.CSS_SELECTOR, 'a[href]')
        for a in anchors:
            try:
                if not a.is_displayed():
                    continue
                if self._is_within_blacklisted_section(a):
                    continue
                href = a.get_attribute('href')
                if not self._is_potential_product_href(href, base_url):
                    continue
                # Require image in the anchor or immediate container
                has_img = False
                image_el = None
                try:
                    image_el = a.find_element(By.CSS_SELECTOR, 'img[src], img[data-src], img[data-original], img[data-srcset]')
                    has_img = True
                except Exception:
                    try:
                        parent = a.find_element(By.XPATH, './..')
                        image_el = parent.find_element(By.CSS_SELECTOR, 'img[src], img[data-src], img[data-original], img[data-srcset]')
                        has_img = True
                    except Exception:
                        pass
                if not has_img:
                    continue
                title = self._clean_text(a.get_attribute('title') or a.text)
                image_url = None
                if image_el:
                    image_url = (
                        image_el.get_attribute('src')
                        or image_el.get_attribute('data-src')
                        or image_el.get_attribute('data-original')
                        or image_el.get_attribute('data-srcset')
                    )
                product = {
                    'title': title,
                    'product_url': self._to_absolute(base_url, href),
                    'image_url': self._to_absolute(base_url, image_url) if image_url else None,
                    'price': None,
                    'currency': None,
                    'raw_price': None,
                    'rating': None,
                    'review_count': None,
                    'in_stock': None,
                    'brand': None,
                    'sku': None,
                    'description': None,
                }
                if self._is_valid_product(product, base_url):
                    products.append(product)
                    if len(products) >= max_items:
                        break
            except Exception:
                continue
        return products

    # ------------------------------ Validations -------------------------------

    def _is_within_blacklisted_section(self, element) -> bool:
        if element is None:
            return False
        try:
            current = element
            for _ in range(6):
                tag = current.tag_name.lower()
                if tag in self.blacklisted_sections:
                    return True
                if tag in ('body', 'html'):
                    break
                parent = current.find_element(By.XPATH, '..')
                if parent is current:
                    break
                current = parent
        except Exception:
            return False
        return False

    def _is_valid_product(self, product: Dict[str, Any], base_url: str) -> bool:
        url = product.get('product_url')
        title = self._clean_text(product.get('title')) if product.get('title') else None

        if not url:
            return False
        if self._is_blacklisted_link(url):
            return False
        if not self._is_product_like_path(url, base_url) and not (product.get('price') and title):
            return False
        if title and (self._looks_like_phone_or_nav(title) or len(title) < 2):
            return False
        if not title and not product.get('price') and not product.get('raw_price'):
            return False
        return True

    def _is_blacklisted_link(self, href: str) -> bool:
        if not href:
            return True
        h = href.lower()
        if any(h.startswith(prefix) for prefix in ('javascript:', 'mailto:', 'tel:')):
            return True
        return any(keyword in h for keyword in self.link_blacklist_keywords)

    def _is_product_like_path(self, href: str, base_url: str) -> bool:
        try:
            parsed = urlparse(href)
            path = (parsed.path or '').lower()
            query = (parsed.query or '').lower()
            fragment = (parsed.fragment or '').lower()

            if path in ('', '/', '/home', '/index', '/index.html'):
                return False

            combined = f"{path}?{query}#{fragment}"
            if any(keyword in combined for keyword in self.product_path_keywords):
                return True

            negative_keywords = ['search', 'account', 'contact', 'login', 'register', 'wishlist', 'cart', 'help', 'support', 'faq', 'privacy', 'terms']
            if any(neg in combined for neg in negative_keywords):
                return False

            if path.endswith('.html') or path.endswith('.htm'):
                return True
            if path.count('/') >= 2 and len(path) > 3:
                return True
            if '-' in path and len(path.replace('-', '')) > 6:
                return True
            return False
        except Exception:
            return False

    def _is_potential_product_href(self, href: Optional[str], base_url: str) -> bool:
        if not href:
            return False
        if self._is_blacklisted_link(href):
            return False
        return self._is_product_like_path(href, base_url)

    def _looks_like_phone_or_nav(self, text: str) -> bool:
        if not text:
            return False
        t = text.lower()
        if re.search(r"\b\+?\d{8,}\b", t):  # long phone numbers
            return True
        nav_words = [
            'home', 'about', 'contact', 'help', 'account', 'login', 'register', 'signup',
            'wishlist', 'cart', 'track', 'order', 'policy', 'privacy', 'terms', 'faq',
            'support', 'customer care', 'service', 'blog', 'news', 'store locator'
        ]
        return any(n in t for n in nav_words)

    def _extract_price_from_text(self, text: Optional[str]) -> Optional[str]:
        if not text:
            return None
        t = text.strip()
        m = re.search(
            r"((?:₹|rs\.?|rs\s|inr\s|usd\s|eur\s|cad\s|aud\s|£|€|\$)\s*[\d,.]+(?:\.\d{1,2})?)",
            t,
            flags=re.IGNORECASE,
        )
        return m.group(1) if m else None

    def _dedupe_by_url(self, products: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        aggregated: Dict[str, Dict[str, Any]] = {}
        order: List[str] = []
        for p in products:
            url = p.get('product_url')
            if not url:
                continue
            if url not in aggregated:
                aggregated[url] = dict(p)
                order.append(url)
            else:
                existing = aggregated[url]
                for key, value in p.items():
                    if key == '_element':
                        continue
                    if value and not existing.get(key):
                        existing[key] = value
        return [aggregated[u] for u in order]

    def _clean_text(self, text: Optional[str]) -> Optional[str]:
        if text is None:
            return None
        cleaned = re.sub(r"\s+", " ", text).strip()
        return cleaned or None

    def _parse_price(self, raw: Optional[str]) -> (Optional[float], Optional[str]):
        if not raw:
            return None, None
        txt = raw.strip()
        currency = None
        # Detect common currency symbols/keywords
        lowered = txt.lower()
        if any(sym in lowered for sym in ["₹", "rs", "rs.", "inr"]):
            currency = "INR"
        elif "$" in txt or "usd" in lowered:
            currency = "USD"
        elif "€" in txt or "eur" in lowered:
            currency = "EUR"
        elif "£" in txt or "gbp" in lowered:
            currency = "GBP"
        elif "cad" in lowered:
            currency = "CAD"
        elif "aud" in lowered:
            currency = "AUD"
        # Extract number
        num_match = re.findall(r"[\d,.]+", txt)
        if not num_match:
            return None, currency
        num = num_match[0].replace(",", "")
        try:
            return float(num), currency
        except Exception:
            return None, currency

    def _parse_rating(self, raw: Optional[str]) -> Optional[float]:
        return self._parse_float(raw)

    def _parse_int(self, raw: Optional[str]) -> Optional[int]:
        if not raw:
            return None
        m = re.findall(r"\d+", str(raw))
        if not m:
            return None
        try:
            return int(m[0])
        except Exception:
            return None

    def _parse_float(self, raw: Optional[str]) -> Optional[float]:
        if not raw:
            return None
        m = re.findall(r"[\d.]+", str(raw))
        if not m:
            return None
        try:
            return float(m[0])
        except Exception:
            return None

    def _infer_in_stock(self, availability_text: Optional[str]) -> Optional[bool]:
        if availability_text is None:
            return None
        t = availability_text.lower()
        if any(k in t for k in ["in stock", "instock", "available", "availabilityinstock"]):
            return True
        if any(k in t for k in ["out of stock", "outofstock", "unavailable"]):
            return False
        return None

    def _to_absolute(self, base_url: str, href: Optional[str]) -> Optional[str]:
        if not href:
            return None
        try:
            return urljoin(base_url, href)
        except Exception:
            return href

    # ----------------------------- Database Operations -----------------------------

    def _save_products_to_db(self, products: List[Dict[str, Any]], platform_url: str, platform: str,
                           category_id: Optional[int] = None, searched_product_id: Optional[int] = None) -> int:
        """
        Save extracted products to the product_data table in Supabase
        
        Args:
            products: List of product dictionaries
            platform_url: URL of the platform/page where products were extracted
            platform: Platform domain name
            category_id: ID of the category this search belongs to (optional)
            searched_product_id: ID of the product from products table that was searched for (optional)
            
        Returns:
            Number of products successfully saved
        """
        if not self.supabase:
            print("[!] Supabase not available - products not saved to database")
            return 0
        
        if not products:
            print("[!] No products to save")
            return 0
        
        saved_count = 0
        failed_count = 0
        
        print(f"\n[*] Saving {len(products)} products to database...")
        
        for product in products:
            try:
                # Validate and sanitize rating (must be between 0 and 100)
                rating = product.get("rating")
                if rating is not None:
                    try:
                        rating_float = float(rating)
                        # Clamp rating between 0 and 100 (some sites use 0-10, some 0-5, some 0-100)
                        if rating_float < 0:
                            rating = 0.0
                        elif rating_float > 100:
                            rating = 100.0
                        else:
                            rating = round(rating_float, 2)
                    except (ValueError, TypeError):
                        rating = None
                
                # Validate and sanitize price
                price = product.get("price")
                if price is not None:
                    try:
                        price_float = float(price)
                        # Ensure price is positive and reasonable (max 999999999.99)
                        if price_float < 0:
                            price = None
                        elif price_float > 999999999.99:
                            price = 999999999.99
                        else:
                            price = round(price_float, 2)
                    except (ValueError, TypeError):
                        price = None
                
                # Validate reviews count (must be positive integer)
                reviews = product.get("review_count")
                if reviews is not None:
                    try:
                        reviews_int = int(float(reviews))  # Handle float strings
                        if reviews_int < 0:
                            reviews = None
                        else:
                            reviews = reviews_int
                    except (ValueError, TypeError):
                        reviews = None
                
                # Map extracted fields to database fields
                db_data = {
                    "platform_url": platform_url,
                    "product_name": product.get("title") or "",
                    "original_price": product.get("raw_price"),  # Keep as text for display
                    "current_price": price,
                    "product_url": product.get("product_url") or "",
                    "product_image_url": product.get("image_url"),
                    "description": product.get("description"),
                    "rating": rating,
                    "reviews": reviews,
                    "in_stock": product.get("in_stock"),
                    "brand": product.get("brand"),
                    "category_id": category_id,
                    "searched_product_id": searched_product_id,
                }
                
                # Skip if required fields are missing
                if not db_data["product_name"] or not db_data["product_url"]:
                    print(f"[!] Skipping product - missing required fields (name or URL)")
                    failed_count += 1
                    continue
                
                # Insert into database
                # If product_url has unique constraint, duplicates will be handled by database
                response = self.supabase.table("product_data").insert(db_data).execute()
                
                if response.data:
                    saved_count += 1
                    if saved_count % 10 == 0:
                        print(f"[*] Saved {saved_count} products so far...")
                else:
                    failed_count += 1
                    
            except Exception as e:
                error_msg = str(e).lower()
                # Handle duplicate key errors gracefully (if product_url has unique constraint)
                if "duplicate" in error_msg or "unique" in error_msg or "constraint" in error_msg:
                    # Product already exists, skip silently
                    saved_count += 1  # Count as successful since product already exists
                else:
                    print(f"[✗] Error saving product: {e}")
                    failed_count += 1
                continue
        
        print(f"[✓] Saved {saved_count}/{len(products)} products to database")
        if failed_count > 0:
            print(f"[!] Failed to save {failed_count} products")
        
        return saved_count


# ============================================================================
# MAIN EXECUTION (Example)
# ============================================================================

if __name__ == "__main__":
    print("=" * 80)
    print("UNIVERSAL PRODUCT EXTRACTOR")
    print("=" * 80)

    # Example page (Rapid Delivery search result HTML)
    # Ref: https://www.rapiddeliveryservices.in/looking-for-saree.html
    test_page = "https://thelittle.in/categories/school-stationery"

    extractor = UniversalProductExtractor()
    result = extractor.extract_products(test_page, max_items=100)

    if result.get("success"):
        print(f"Platform: {result.get('platform')}")
        print(f"Products found: {result.get('num_products')}")
        print("\n" + "=" * 80)
        for i, p in enumerate(result.get("products", [])[:10], 1):
            print(f"\n[Product {i}]")
            print(f"  Title: {p.get('title')}")
            print(f"  URL: {p.get('product_url')}")
            print(f"  Image: {p.get('image_url')}")
            print(f"  Price: {p.get('price')} {p.get('currency') or ''} (raw: {p.get('raw_price') or 'N/A'})")
            print(f"  Rating: {p.get('rating') or 'N/A'}")
            print(f"  Reviews: {p.get('review_count') or 'N/A'}")
            print(f"  In Stock: {p.get('in_stock') if p.get('in_stock') is not None else 'Unknown'}")
            print(f"  Brand: {p.get('brand') or 'N/A'}")
            print(f"  SKU: {p.get('sku') or 'N/A'}")
            print(f"  Description: {(p.get('description') or 'N/A')[:100]}...")
            print("-" * 80)
    else:
        print("Extraction failed:", result.get("error"))


