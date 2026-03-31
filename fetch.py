import os
import re
import gzip
import random
import logging
import asyncio
import traceback
import urllib.parse
from typing import Optional, List, Dict, Any, Callable, Tuple
from dataclasses import dataclass
from datetime import datetime, UTC

import pytz
import aiohttp
import diskcache
import redis
from bs4 import BeautifulSoup
from lxml import html
from playwright.async_api import async_playwright, Browser, Page, BrowserContext
from browserlib.injectors.playwright import AsyncNewContext
from browserlib.fingerprints import FingerprintGenerator

# Configure standard logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

__VERSION__ = "0.3.0"


class BSParser:
    """Utility class for parsing HTML content using BeautifulSoup."""
    def __init__(self, content: str, url: str, parser: str = "lxml"):
        self.domain = urllib.parse.urlparse(url).netloc
        self.soup = BeautifulSoup(content, parser)

    def select(self, selector: str, find_all: bool = False) -> Any:
        """Robust selector method for standardizing data extraction."""
        if find_all:
            return self.soup.select(selector)
        return self.soup.select_one(selector)


@dataclass
class CacheConfig:
    """Configuration for multi-tier caching behavior."""
    use_redis: bool = True
    use_disk_cache: bool = True
    redis_url: str = os.getenv("REDIS_URL", "redis://localhost:6379")
    redis_db: int = 0
    cache_limit_minutes: int = 525600  # 365 days
    disk_cache_dir: Optional[str] = None
    disk_cache_shards: int = 4
    disk_cache_size_limit: int = 200 * 1024 ** 3  # 200GB
    clean_html_on_cache: bool = True

    def __post_init__(self):
        if self.disk_cache_dir is None:
            data_dir = os.getenv('BROWSER_CACHE', os.path.join(os.path.expanduser("~"), ".cache", "fetch_libs"))
            self.disk_cache_dir = os.path.join(data_dir, 'fanout_cache')


class CacheManager:
    """Handles multi-tier caching (Redis for fast access, Disk for massive scale)."""
    def __init__(self, config: CacheConfig):
        self.config = config
        self.disk_cache = diskcache.FanoutCache(
            self.config.disk_cache_dir,
            shards=self.config.disk_cache_shards,
            timeout=1,
            size_limit=self.config.disk_cache_size_limit
        )
        self.redis_cache = redis.from_url(
            self.config.redis_url,
            db=self.config.redis_db,
            decode_responses=True,
            socket_connect_timeout=10
        )

    def get(self, key: str) -> Tuple[bool, Optional[datetime], Optional[str]]:
        # Tier 1: Redis Check
        exists_redis: str = self.redis_cache.get(key)
        if exists_redis:
            # Using \x1e (Record Separator) for highly efficient parsing over JSON
            fetch_date_str, content = exists_redis.split("\x1e", 1)
            fetch_date = datetime.fromtimestamp(float(fetch_date_str), tz=UTC)
            logger.debug(f"[redis] Cache hit for {key}")
            if self._is_cache_valid(fetch_date):
                return True, fetch_date, content

        # Tier 2: Disk Cache Check
        exists_disk: dict = self.disk_cache.get(key, retry=True)
        if exists_disk:
            logger.debug(f"[disk] Cache hit for {key}")
            if self._is_cache_valid(exists_disk["t"]):
                return True, exists_disk["t"], exists_disk["d"]
                
        return False, None, None

    def set(self, key: str, content: str) -> None:
        t = datetime.now(UTC)
        self.disk_cache[key] = {"d": content, "t": t}
        self.redis_cache.set(key, f"{t.timestamp()}\x1e{content}")

    def _is_cache_valid(self, fetch_date: datetime) -> bool:
        if fetch_date.tzinfo is None:
            fetch_date = fetch_date.replace(tzinfo=pytz.utc)
        age_seconds = (datetime.now(UTC) - fetch_date).total_seconds()
        return age_seconds <= (self.config.cache_limit_minutes * 60)
        
    def close(self):
        self.disk_cache.close()
        self.redis_cache.close()


def clean_html_with_lxml(html_content: str) -> str:
    """Strips unnecessary bloat (styles, scripts, trackers) from HTML to save cache space."""
    doc = html.fromstring(html_content)

    for element in doc.xpath('//style | //path | //link'):
        element.getparent().remove(element)

    for element in doc.xpath('//*[@style or @onclick or starts-with(@*, "on")]'):
        if 'style' in element.attrib:
            del element.attrib['style']
        for attr in list(element.attrib.keys()):
            if attr.startswith('on'):
                del element.attrib[attr]

    html_str = html.tostring(doc, encoding='unicode')
    html_str = re.sub(r'\n\s*\n+', '\n', html_str)
    return html_str


class SessionManager:
    """Manages an aiohttp session singleton with safe async locking."""
    def __init__(self):
        self._session: Optional[aiohttp.ClientSession] = None
        self._lock = asyncio.Lock()

    async def get(self) -> aiohttp.ClientSession:
        if self._session is not None and not self._session.closed:
            return self._session
        async with self._lock:
            if self._session is None or self._session.closed:
                self._session = aiohttp.ClientSession(
                    connector=aiohttp.TCPConnector(limit=200),
                    timeout=aiohttp.ClientTimeout(total=300)
                )
            return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()


class RequestArgs:
    """Handles dynamic generation of headers, proxies, and rotational states."""
    def __init__(self, proxies: List[str] = None, user_agents: List[str] = None, use_proxies: bool = True, override_fn: Callable = None):
        self.use_proxies = use_proxies
        self.proxies = proxies or []
        self.user_agents = user_agents or [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ]
        
        self.proxy_weights = [1_000_000 if "shifter" in p else 2_250_000 for p in self.proxies] if self.proxies else []
        self.ua_weights = [1_000_000] * len(self.user_agents)
        self.override_fn = override_fn

    def generate(self, url: str) -> Dict[str, Any]:
        headers = random.choices(self.user_agents, weights=self.ua_weights)[0] if self.user_agents else {}
        req = {"method": "GET", "url": url, "headers": {"User-Agent": headers}, "proxy": self.generate_proxy()}
        if self.override_fn:
            req.update(self.override_fn(req, "http"))
        return req

    def generate_pw_args(self, url: str) -> Dict[str, Any]:
        req = {"url": url}
        if self.override_fn:
            req.update(self.override_fn(req, "pw"))
        return req

    def generate_proxy(self, format_type: str = "") -> Optional[Any]:
        if not self.use_proxies or not self.proxies:
            return None
            
        proxy = random.choices(self.proxies, weights=self.proxy_weights)[0]
        try:
            ip, port, user, pwd = proxy.split(':')
            if format_type == "playwright":
                return {"server": f"{ip}:{port}", "username": user, "password": pwd}
            return f"http://{user}:{pwd}@{ip}:{port}"
        except ValueError:
            logger.warning(f"Malformed proxy string: {proxy}")
            return proxy


class ARequest:
    """Context manager handling the actual fetching logic, routing between standard HTTP and Headless Browsers."""
    def __init__(self, session_manager: SessionManager, cache_manager: CacheManager, req_args: RequestArgs, **kwargs):
        self.req_args = req_args
        self.cache_manager = cache_manager
        self.session_manager = session_manager
        
        self.max_retries = kwargs.get("max_retries", 3)
        self.skip_rc = [404, 400, 410] + kwargs.get('skip_rc', [])
        self.valid_rc = [301, 302, 307] + kwargs.get('valid_rc', [])
        
        self.browser: Optional[Browser] = kwargs.get("browser")
        self.fingerprints = kwargs.get("fingerprints")
        self.use_proxies = kwargs.get("use_proxies", True)
        
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None
        
        self.error_cooldown = 1
        self.sleep_time = 1
        self.block_types = ["media", "image", "font", "stylesheet", "script", "xhr", "fetch", "other"]

        # Request state
        self._reset_state("", "")

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.context:
            await self.context.close()

    def _reset_state(self, url: str, f_url: str = None):
        self.url = url
        self.f_url = f_url or url
        self.rc = None
        self.success = False
        self.content = ""
        self.fetch_date = None
        self.retries = 0

    async def fetch(self, url: str, **kwargs) -> 'ARequest':
        self._reset_state(url, kwargs.get('f_url'))
        if await self._check_cache():
            return self
            
        while self.retries < self.max_retries and not self.success:
            req = self.req_args.generate(url)
            try:
                session = await self.session_manager.get()
                async with session.request(**req) as r:
                    self.rc = r.status
                    if self.rc in self.skip_rc:
                        break
                    self.success = self.rc // 100 == 2 or self.rc in self.valid_rc
                    self.content = await r.text()
                    self.fetch_date = datetime.now(UTC)
                    await asyncio.sleep(self.sleep_time if self.success else 0)
                    self._log()
            except Exception as e:
                logger.debug(f"HTTP Fetch Error: {e.__class__.__name__}: {e.args}")
                self._log()
                
            self.retries += 1
            if not self.success:
                await asyncio.sleep(self.error_cooldown)
                
        self._update_cache()
        return self

    async def _restart_context(self):
        proxy = self.req_args.generate_proxy("playwright") if self.use_proxies else None
        if self.context:
            await self.context.close()
            
        try:
            self.context = await AsyncNewContext(
                self.browser, 
                fingerprint=self.fingerprints.generate(),
                ignore_https_errors=True,
                proxy=proxy
            )
        except ValueError:
            logger.warning("Failed to generate advanced fingerprint; falling back to standard context.")
            self.context = await self.browser.new_context(proxy=proxy)
            
        self.page = await self.context.new_page()
        await self.page.route("**/*", self._block_resources_by_type)

    async def _block_resources_by_type(self, route):
        """Optimizes browser execution by aborting unnecessary resource loads."""
        if route.request.resource_type in self.block_types:
            await route.abort()
        else:
            await route.continue_()

    async def fetch_browser(self, url: str) -> 'ARequest':
        self._reset_state(url)
        if await self._check_cache():
            return self
            
        if self.context is None:
            await self._restart_context()
            
        while self.retries < self.max_retries and not self.success:
            try:
                # Specialized logic for forced direct downloads in headless contexts
                if url.endswith(".xml.gz"):
                    filepath = os.path.join(self.cache_manager.config.disk_cache_dir, os.path.basename(urllib.parse.urlparse(url).path))
                    if not os.path.exists(filepath):
                        async with self.page.expect_download(timeout=120000) as download_info:
                            await self.page.evaluate(f"() => {{ const a = document.createElement('a'); a.href = '{url}'; a.download = ''; document.body.appendChild(a); a.click(); a.remove(); }}")
                        download = await download_info.value
                        await download.save_as(filepath)
                        
                    with gzip.open(filepath, "rt", encoding="utf-8") as f:
                        self.content = f.read()
                    self.rc = 200
                    self.success = True
                    break
                else:
                    r = await self.page.goto(**self.req_args.generate_pw_args(url))
                    self.fetch_date = datetime.now(UTC)
                    self.rc = r.status
                    if 200 <= r.status < 400 or r.status in self.valid_rc:
                        self.success = True
                        self.content = await self.page.content()
                        break
                        
            except Exception as e:
                logger.debug(f"Browser Fetch Error: {e.__class__.__name__}: {e.args} {url}")
                self.rc = 999
                
            await asyncio.sleep(self.error_cooldown)
            self.retries += 1
            await self._restart_context()
            
        self._log()
        self._update_cache()
        return self

    async def _check_cache(self) -> bool:
        self.success = False
        if self.cache_manager:
            self.success, self.fetch_date, self.content = self.cache_manager.get(self.f_url)
        return self.success

    def _update_cache(self):
        if self.success and self.cache_manager and self.content:
            content_to_cache = self.content
            if isinstance(content_to_cache, str) and "<html" in content_to_cache.lower():
                content_to_cache = clean_html_with_lxml(content_to_cache)
            self.cache_manager.set(self.f_url, content_to_cache)

    def _log(self):
        log_func = logger.info if self.success else logger.warning
        log_func(f"[{self.rc}] [{self.success}] [Retry: {self.retries}] {self.f_url}")


class Scraper:
    """Main orchestration class managing concurrent worker queues for extraction."""
    def __init__(self, cache_dir: Optional[str] = None, max_workers: int = 10, proxies: List[str] = None):
        self.fingerprints = FingerprintGenerator()
        self.cache_manager = CacheManager(CacheConfig(disk_cache_dir=cache_dir))
        self.max_workers = max_workers
        self.req_args = RequestArgs(proxies=proxies)
        
        self.browser: Optional[Browser] = None
        self.session_manager = SessionManager()
        self.queue: asyncio.Queue = asyncio.Queue()
        
        self.use_proxies = True
        self.skip_failed_pages = True

    async def scrape(self, urls: List[str], parse_fn: Callable = None, use_browser: bool = False, **kwargs) -> List[Any]:
        if not urls:
            return []
            
        self.use_proxies = kwargs.get("use_proxies", True)
        self.skip_failed_pages = kwargs.get("skip_failed_pages", True)
        
        for url in urls:
            await self.queue.put(url)
            
        async with async_playwright() as p:
            self.browser = await p.firefox.launch(
                headless=kwargs.get("headless", True),
                proxy=self.req_args.generate_proxy("playwright") if self.use_proxies else None
            )

            results = []

            async def worker():
                while True:
                    try:
                        url = await asyncio.wait_for(self.queue.get(), timeout=2.0)
                    except asyncio.TimeoutError:
                        break  # Queue empty, shut down worker
                        
                    if url is None:
                        self.queue.task_done()
                        break
                        
                    try:
                        res = await self.fetch_url([url], use_browser, parse_fn)
                        results.extend(res)
                    finally:
                        self.queue.task_done()

            try:
                workers = [asyncio.create_task(worker()) for _ in range(self.max_workers)]
                await self.queue.join()
                
                # Send poison pills to shut down workers gracefully
                for _ in workers:
                    await self.queue.put(None)
                await asyncio.gather(*workers)
                
            finally:
                await self.browser.close()
                await self.session_manager.close()
                self.cache_manager.close()

        return results

    async def fetch_url(self, urls: List[str], use_browser: bool, parse_fn: Callable) -> List[Any]:
        results = []
        async with ARequest(self.session_manager, self.cache_manager, self.req_args, 
                            fingerprints=self.fingerprints, browser=self.browser, 
                            use_proxies=self.use_proxies) as r:
            for url in urls:
                if use_browser:
                    await r.fetch_browser(url)
                else:
                    await r.fetch(url)

                if not r.success and self.skip_failed_pages:
                    continue

                result = {
                    "status_code": r.rc,
                    "url": url,
                    "fetch_date": r.fetch_date,
                    "content": r.content
                }

                if parse_fn:
                    try:
                        parsed = parse_fn(result)
                        # Support for recursive crawling: if parser returns (data, new_urls)
                        if isinstance(parsed, tuple) and len(parsed) == 2:
                            for new_url in parsed[1]:
                                await self.queue.put(new_url)
                            parsed = parsed[0]
                        result = parsed
                    except Exception as e:
                        logger.error(f"Parse error on {url}: {e}\n{traceback.format_exc()}")
                        continue

                if isinstance(result, list):
                    results.extend(result)
                elif result:
                    results.append(result)
                    
        return results
