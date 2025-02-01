from collections.abc import Iterable
import scrapy
from parsel import Selector
from playwright.async_api import Page
from scrapy import Request
from scrapy.http import Response


class Article(scrapy.Item):
    title = scrapy.Field()
    pub_date = scrapy.Field()
    author = scrapy.Field()
    header_photo_url = scrapy.Field(default=None)
    header_photo_base64 = scrapy.Field(default=None)
    description = scrapy.Field()
    source_url = scrapy.Field()
    keywords = scrapy.Field()
    article_text = scrapy.Field()


PAGINATION_SIZE = 25


def should_abort_request(request):
    return "yandex" in request.url or "ya" in request.url or "google" in request.url or "smi2" in request.url


class KpNewsSpider(scrapy.Spider):
    name = "kp_news"
    allowed_domains = ["kp.ru"]
    required_articles_count = 1025
    total_scanned_articles = 0

    custom_settings = {
        "ITEM_PIPELINES": {"kpru.pipelines.PhotoDownloaderPipeline": 100,
                           "kpru.pipelines.MongoPipeline": 200},
        "MONGO_DB": "kpru",
        "MONGO_USER": "admin12",
        "MONGO_PASSWORD": "adminpass12",
        "MONGO_DB_COLLECTION": "articles",
        "PLAYWRIGHT_ABORT_REQUEST": should_abort_request,
        "PLAYWRIGHT_LAUNCH_OPTIONS": {"headless": False},
        "DOWNLOAD_HANDLERS": {
            "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
            "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
        },
    }

    def start_requests(self) -> Iterable[Request]:
        yield scrapy.Request(
            url="https://www.kp.ru/online/",
            meta={"playwright": True, "playwright_include_page": True},
        )

    async def parse(self, response: Response):
        page: Page = response.meta["playwright_page"]
        while self.total_scanned_articles < self.required_articles_count:
            page_selector = Selector(await page.content())
            urls = page_selector.xpath("//a [@class='sc-1tputnk-2 drlShK']/@href").getall()[-PAGINATION_SIZE:]
            for url in urls:
                print("GO TO", response.urljoin(url))
                yield scrapy.Request(url=response.urljoin(url), callback=self.parse_page)

            await page.locator("button:has-text('Показать еще')").click(position={"x": 176, "y": 26.5})
            await page.wait_for_timeout(10000)
            self.total_scanned_articles += PAGINATION_SIZE
            print('SCANNED:', self.total_scanned_articles)
        await page.close()

    def parse_page(self, response):
        item = {}
        item['title'] = ''
        if (response.xpath("//h1/span/text()").get()):
            item['title'] += response.xpath("//h1/span/text()").get()
        if (response.xpath("//h1/text()").get()):
            item['title'] += response.xpath("//h1/text()").get()
        item['pub_date'] = response.xpath(
            "//span [@class='sc-j7em19-1 dtkLMY']/text()").get()
        item['author'] = response.xpath(
            "//a [@class='sc-1jl27nw-4 fsKCGr']/span/text()").get()
        item['header_photo_url'] = response.xpath(
            "//div [@data-content-type='photo']/@data-content-src").get()
        item['description'] = response.xpath(
            "//div [@class='sc-j7em19-4 nFVxV']/text()").get()
        item['source_url'] = response.url
        item['keywords'] = response.xpath(
            "//div [@class='sc-j7em19-2 dQphFo']/a/text()").getall()
        text = response.xpath(
            '//div[@data-gtm-el="content-body"]//p//text()').getall()
        item['article_text'] = ' '.join(text).strip()
        # print('--------------------------')
        # print(item)
        # print('--------------------------')
        return item
