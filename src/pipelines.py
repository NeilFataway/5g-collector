# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import sqlite3


class FiveGPipeline:
    colleciton_name = "5g_pipeline"

    def __init__(self, sqlite_db_file):
        self.sqlite_db_file = sqlite_db_file
        self.porn_names = {}
        self.tag_names = {}
        self.db = None

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            sqlite_db_file=crawler.settings.get("SQLITE_DB_FILE")
        )

    def open_spider(self, spider):
        self.db = sqlite3.Connection(self.sqlite_db_file, timeout=60)
        self.db.cursor().executescript("""
            BEGIN;
            CREATE TABLE IF NOT EXISTS porns (
                `id` INTEGER PRIMARY  KEY AUTOINCREMENT,
                `name` CHAR(128), 
                `image` VARCHAR(2048),
                `m3u8` VARCHAR(2048),
                `video` VARCHAR(2048),
                `area` CHAR(10),
                `origin` VARCHAR(2048)
            );
            CREATE TABLE IF NOT EXISTS tags (
                `id` INTEGER PRIMARY  KEY AUTOINCREMENT,
                `name` CHAR(20)
            );
            CREATE TABLE IF NOT EXISTS porn2tag (
                porn_id INTEGER CONSTRAINT porn_fk REFERENCES porns(`id`),
                tag_id  INTEGER CONSTRAINT tag_fk REFERENCES tgs(`id`)
            );
            COMMIT;
        """)
        porn_names = self.db.cursor().execute("SELECT `id`, name FROM porns").fetchall()
        tags = self.db.cursor().execute("SELECT `id`, name FROM tags").fetchall()
        self.porn_names = {name: ID for ID, name in porn_names}
        self.tag_names = {name: ID for ID, name in tags}

    def close_spider(self, spider):
        if self.db:
            self.db.close()

    def process_item(self, item, spider):
        if spider.name != "5g-spider":
            return item

        adapter = ItemAdapter(item)
        if adapter.get("name") in self.porn_names:
            return item

        cur = self.db.cursor()
        cur.execute(
            "INSERT INTO porns(name, image, m3u8, video, area, origin) VALUES (?, ?, ?, ?, ?, ?)",
            (item.get("name"),
             item.get("image"),
             item.get("m3u8"),
             item.get("video"),
             item.get("area"),
             item.get("origin"))
        )
        porn_id = cur.lastrowid

        tags = self.get_extra_tags(item)

        new_tags = {}

        def get_tag_id(tag_name):
            if tag_name in self.tag_names:
                return self.tag_names[tag_name]
            if tag_name in new_tags:
                return new_tags[tag_name]

        for tag_name in tags:
            if tag_name not in self.tag_names and tag_name not in new_tags:
                cur.execute("INSERT INTO tags(name) VALUES (?)", (tag_name,))
                new_tags[tag_name] = cur.lastrowid
            cur.execute("INSERT INTO porn2tag(porn_id, tag_id) VALUES (?, ?)", (porn_id, get_tag_id(tag_name)))
        self.db.commit()
        self.porn_names[item.get("name")] = cur.lastrowid
        self.tag_names.update(new_tags)

        return item

    @staticmethod
    def get_extra_tags(item):
        tags = item.get("tags")
        if "无码" in item.get("name") and "无码" not in tags:
            tags.append("无码")
        if "有码" in item.get("name") and "有码" not in tags:
            tags.append("有码")
        return tags
