import base64
from io import BytesIO
from PIL import Image
import pymongo
from itemadapter import ItemAdapter

import aiohttp
from aiohttp.client_exceptions import InvalidUrlClientError


class PhotoDownloaderPipeline:
    def __init__(self, result_image_quality: int):
        self.result_image_quality = result_image_quality

    @classmethod
    def from_crawler(cls, crawler):
        result_image_quality = crawler.settings.get("RESULT_IMAGE_QUALITY", 35)
        return cls(result_image_quality=result_image_quality)

    def compress_image(self, image_content: bytes):
        input_buffer = BytesIO(image_content)
        output_buffer = BytesIO()
        img = Image.open(input_buffer)
        if img.mode in ("RGBA", "P"):
            img = img.convert("RGB")
        img.save(output_buffer, format="JPEG", quality=self.result_image_quality, optimize=True)
        return output_buffer.getvalue()

    async def _download_photo_to_base64(self, url: str):
        async with aiohttp.ClientSession() as session:
            response = await session.get(url)
            if response.status != 200:
                return ""
            content = await response.read()
            compressed_bytes = self.compress_image(image_content=content)
            encoded_image = base64.b64encode(compressed_bytes).decode("utf-8")
            return encoded_image

    async def process_item(self, item, spider):
        if item["header_photo_url"]:
            try:
                photo_base64 = await self._download_photo_to_base64(item["header_photo_url"])
            except InvalidUrlClientError:
                item["header_photo_url"] = None
                return item
            item["header_photo_base64"] = photo_base64

            return item
        return item


class MongoPipeline:
    collection_name = "kpru"

    def __init__(self, mongo_uri, mongo_db, mongo_db_collection):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.mongo_db_collection = mongo_db_collection

    @classmethod
    def from_crawler(cls, crawler):
        mongo_db = crawler.settings.get("MONGO_DB", "kpru")
        mongo_user = crawler.settings.get("MONGO_USER")
        mongo_host = crawler.settings.get("MONGO_HOST", "localhost")
        mongo_password = crawler.settings.get("MONGO_PASSWORD")
        mongo_port = crawler.settings.get("MONGO_PORT", 27017)
        mongo_db_collection = crawler.settings.get(
            "MONGO_DB_COLLECTION", "articles")
        mongo_uri = f"mongodb://{mongo_user}:{mongo_password}@localhost"

        return cls(
            mongo_uri=mongo_uri,
            mongo_db=mongo_db, mongo_db_collection=mongo_db_collection)

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):

        self.db[self.mongo_db_collection].insert_one(ItemAdapter(item).asdict())
        return item
