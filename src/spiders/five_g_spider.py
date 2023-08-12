import scrapy


class FiveGSpider(scrapy.Spider):
    name = '5g-spider'
    start_urls = ["https://5satke.top/"]

    def parse(self, response, **kwargs):
        allhrefs = response.css("li.pin a::attr(href)").getall()
        allhrefs = filter(lambda x: x != "#", set(allhrefs))
        for item in allhrefs:
            video_page = response.urljoin(item)
            yield scrapy.Request(video_page, callback=self.parse_video)

        next_page = response.urljoin(response.xpath('//a[@aria-label="下页"]/@href').get())
        yield scrapy.Request(next_page, callback=self.parse)

    def parse_video(self, response, **kwargs):
        name = response.xpath('//div[@class="b_t"][.//span[@id="downloadurl"]]/text()').get()
        area = response.css("div.b_t a[href='/']+a::text").get()
        tags = response.css("div.w_z>a::text").getall()
        tags = list(map(lambda x: x.strip(), tags))
        yield {
            "m3u8": response.urljoin(response.css("span#vpath::text").get().strip()),
            "image": response.urljoin(response.css("span#purl::text").get().strip()),
            "video": response.urljoin(response.css("span#downloadurl::text").get().strip()),
            "origin": response.url,
            "tags": tags,
            "name": name,
            "area": area
        }
