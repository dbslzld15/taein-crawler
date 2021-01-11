import datetime
import random
import re
import time
import typing

import attr
import pytz
import structlog
from crawler.aws_client import S3Client
from crawler.utils.download import write_file
from dateutil.relativedelta import relativedelta
from taein_crawler.client import TaeinClient
from taein_crawler.client.data import TaeinRegion, TaeinGugun
from tanker.slack import SlackClient
from tanker.utils.datetime import tznow, timestamp
from tanker.utils.tempfile import TempDir

from .data import CrawlerStatistics, slack_failure_percentage_statistics
from .exc import TaeinCrawlerNotFoundError

logger = structlog.get_logger(__name__)

SeoulTZ = pytz.timezone("Asia/Seoul")


class TaeinCrawler(object):
    def __init__(
        self,
        config: typing.Dict[str, typing.Any],
    ):
        super().__init__()
        self.config = config
        self.slack_client = SlackClient(
            config.get("SLACK_CHANNEL"), config.get("SLACK_API_TOKEN")
        )
        self.taein_client = TaeinClient(
            client_delay=self.config['CLIENT_DELAY'],
            proxy=random.choice(self.config['PROXY_HOST_LIST'])
        )
        self.s3_client = S3Client(config)
        self.total_statistics = CrawlerStatistics()
        self.failure_statistics = CrawlerStatistics()
        self.crawling_date: datetime.datetime = tznow(
            pytz.timezone("Asia/Seoul")
        )
        self.crawling_start_time: str = str(
            timestamp(tznow(pytz.timezone("Asia/Seoul")))
        )

    def run(self, run_by: str) -> None:
        self.slack_client.send_info_slack(
            f"TIME_STAMP: {self.crawling_start_time}\n"
            f"크롤링 시작합니다 "
            f"({self.config['ENVIRONMENT']}, {run_by})"
        )

        self.crawl()
        self.upload_crawler_log_to_s3(run_by)

        statistics = slack_failure_percentage_statistics(
            self.total_statistics, self.failure_statistics
        )

        self.slack_client.send_info_slack(
            f"크롤링 완료\n"
            f"TIME_STAMP: {self.crawling_start_time}\n\n"
            f"statistics:\n"
            f"statistics_count\n{statistics['statistics_count']}\n\n"
            f"bids_count\n{statistics['bids_count']}"
        )

    def crawl(self) -> None:
        login_id = self.config["LOGIN_ID"]
        login_pw = self.config["LOGIN_PW"]

        self.taein_client.fetch_main_page()
        self.taein_client.fetch_login_page()
        self.taein_client.login(login_id, login_pw)

        try:
            logger.info("Crawling region list")

            region = self.taein_client.fetch_region_list()
            if not region:
                raise TaeinCrawlerNotFoundError("not found region list")

            logger.info("Crawling mulgun kind")
            self.crawl_mulgun_kind(region)
        except Exception as e:
            raise e
        finally:
            self.taein_client.logout()

    def crawl_mulgun_kind(self, region: TaeinRegion) -> None:
        end_date_format = self.crawling_date.strftime("%Y-%m-%d")
        end_date = datetime.datetime.strptime(end_date_format, "%Y-%m-%d")
        start_date = end_date - relativedelta(months=1)

        mulgun = self.taein_client.fetch_mulgun_kind_list(start_date, end_date)

        if not mulgun:
            raise TaeinCrawlerNotFoundError("not found mulgun list")

        for mulgun_text, mulgun_value in mulgun.mulgun_kind_dict.items():
            if re.search(f"{self.config['MULGUN_KIND']}$", mulgun_text):
                self.crawl_sido_region(region, mulgun_text, mulgun_value)

    def crawl_sido_region(
        self, region: TaeinRegion, mulgun_text: str, mulgun_value: str
    ) -> None:
        sido_list = region.taein_sido_list

        for sido in sido_list:
            sido_name = sido.sido_name
            if re.search(self.config["REGION_REGEX_LEVEL_1"], sido_name):
                gugun = sido.taein_gugun
                self.crawl_gugun_region(
                    sido_name, gugun, mulgun_text, mulgun_value
                )

    def crawl_gugun_region(
        self,
        sido_name: str,
        gugun: TaeinGugun,
        mulgun_text: str,
        mulgun_value: str,
    ) -> None:
        gugun_name = gugun.gugun_name
        if re.search(self.config["REGION_REGEX_LEVEL_2"], gugun_name):
            self.taein_client = TaeinClient(
                client_delay=self.config['CLIENT_DELAY'],
                proxy=random.choice(self.config['PROXY_HOST_LIST'])
            )
            self.taein_client.login(
                self.config["LOGIN_ID"],
                self.config["LOGIN_PW"]
            )
            dong_list = gugun.dong_list
            self.crawl_dong_region(
                sido_name,
                gugun_name,
                dong_list,
                mulgun_text,
                mulgun_value,
            )
            self.taein_client.logout()
            time.sleep(60)

    def crawl_dong_region(
        self,
        sido_name: str,
        gugun_name: str,
        dong_list: typing.List[str],
        mulgun_text: str,
        mulgun_value: str,
    ) -> None:
        for dong_name in dong_list:
            if re.search(self.config["REGION_REGEX_LEVEL_3"], dong_name):
                area_step = self.config["BUILDING_AREA_STEP"]
                area_start = self.config["BUILDING_AREA_START"]
                area_end = self.config["BUILDING_AREA_END"]
                for area_range in range(
                    area_start, area_end + area_step, area_step
                ):
                    start_area = "최소" if area_range == 0 else area_range
                    end_area = "최대" if area_range == 400 else area_range + 20
                    try:
                        self.crawl_statistics_page(
                            sido_name,
                            gugun_name,
                            dong_name,
                            start_area,
                            end_area,
                            mulgun_text,
                            mulgun_value,
                        )
                    except Exception as e:
                        self.failure_statistics.statistics_count += 1
                        raise e

                self.crawl_bid_page(
                    sido_name,
                    gugun_name,
                    dong_name,
                    mulgun_text,
                    mulgun_value,
                )

    def crawl_statistics_page(
        self,
        sido_name: str,
        gugun_name: str,
        dong_name: str,
        start_area: int,
        end_area: int,
        mulgun_text: str,
        mulgun_value: str,
    ) -> None:
        end_date_format = self.crawling_date.strftime("%Y-%m-%d")
        end_date = datetime.datetime.strptime(end_date_format, "%Y-%m-%d")
        start_date = end_date - relativedelta(months=1)

        statistics_response = self.taein_client.fetch_statistics_page(
            sido_name,
            gugun_name,
            dong_name,
            str(start_area),
            str(end_area),
            start_date,
            end_date,
            mulgun_value,
        )

        if not statistics_response:
            raise TaeinCrawlerNotFoundError("not found statistics response")

        logger.info(
            "Crawling statistics page",
            sido=sido_name,
            gugun=gugun_name,
            dong=dong_name,
            start_area=start_area,
            end_area=end_area,
            mulgun_text=mulgun_text,
            proxy=self.taein_client.session.proxies
        )

        if not statistics_response.dong_statistics_exist:
            logger.info("Not exist dong statistiscs")
            return

        self.total_statistics.statistics_count += 1

        data = statistics_response.raw_data

        file_name = (
            f"{sido_name}_"
            f"{gugun_name}_"
            f"{dong_name}_"
            f"{mulgun_text}_"
            f"{start_area}_"
            f"{end_area}_"
            f"statistics.html"
        )

        with TempDir() as temp_dir:
            temp_path = str(temp_dir) + "\\"
            file_path = temp_path + file_name
            write_file(file_path, data)
            self.upload_page_to_s3(
                sido_name,
                gugun_name,
                dong_name,
                mulgun_text,
                file_path,
                file_name,
                "statistics",
            )

    def crawl_bid_page(
        self,
        sido_name: str,
        gugun_name: str,
        dong_name: str,
        mulgun_text: str,
        mulgun_value: str,
    ) -> None:
        end_date_format = self.crawling_date.strftime("%Y-%m-%d")
        end_date = datetime.datetime.strptime(end_date_format, "%Y-%m-%d")
        start_date = end_date - relativedelta(months=1)

        statistics_response = self.taein_client.fetch_statistics_page(
            sido_name,
            gugun_name,
            dong_name,
            "최소",
            "최대",
            start_date,
            end_date,
            mulgun_value
        )

        if not statistics_response:
            raise TaeinCrawlerNotFoundError("not found statistics response")

        bid_count = statistics_response.bid_count
        total_page = statistics_response.bid_total_page

        if bid_count > 0:
            try:
                for index in range(1, total_page + 1):
                    bid_response = self.taein_client.fetch_bid_list_page(
                        sido_name,
                        gugun_name,
                        dong_name,
                        start_date,
                        end_date,
                        mulgun_text,
                        bid_count,
                        index
                    )

                    if not bid_response:
                        raise TaeinCrawlerNotFoundError(
                            "not found bid response"
                        )

                    self.total_statistics.bids_count += 1

                    data = bid_response.raw_data

                    logger.info(
                        "Crawling bid page",
                        sido=sido_name,
                        gugun=gugun_name,
                        dong=dong_name,
                        mulgun_text=mulgun_text,
                        page_index=index,
                        proxy=self.taein_client.session.proxies
                    )

                    file_name = (
                        f"{sido_name}_"
                        f"{gugun_name}_"
                        f"{dong_name}_"
                        f"{mulgun_text}_"
                        f"bid_{index}.html"
                    )

                    with TempDir() as temp_dir:
                        temp_path = str(temp_dir) + "\\"
                        file_path = temp_path + file_name
                        write_file(file_path, data)
                        self.upload_page_to_s3(
                            sido_name,
                            gugun_name,
                            dong_name,
                            mulgun_text,
                            file_path,
                            file_name,
                            "bid",
                        )
            except Exception as e:
                self.failure_statistics.bids_count += 1
                raise e

    def upload_page_to_s3(
        self,
        sido_name: str,
        gugun_name: str,
        dong_name: str,
        mulgun_text: str,
        file_path: str,
        file_name: str,
        data_type: str,
    ) -> None:
        folder_name = (
            f"{self.config['ENVIRONMENT']}/"
            f"{self.crawling_date.year}/"
            f"{self.crawling_date.month:02}/"
            f"{self.crawling_date.day:02}/"
            f"{str(self.crawling_start_time)}/"
            f"data/"
            f"{sido_name}/"
            f"{gugun_name}/"
            f"{dong_name}/"
            f"{mulgun_text}"
        )

        if data_type == "bid":
            folder_name += f"/{data_type}"

        self.s3_client.upload_any_file(
            folder_name=folder_name,
            file_name=file_name,
            file_path=file_path,
            mime_type="text/html",
            mode="rb",
        )

        logger.info(
            "Upload page to s3",
            sido=sido_name,
            gugun=gugun_name,
            dong=dong_name,
            mulgun_text=mulgun_text,
            data_type=data_type,
        )

    def upload_crawler_log_to_s3(self, run_by: str) -> None:
        total_statistics = attr.asdict(self.total_statistics)
        area_step = self.config["BUILDING_AREA_STEP"]
        area_start = self.config["BUILDING_AREA_START"]
        area_end = self.config["BUILDING_AREA_END"]
        area_range = [
            {"start_area": area_end, "end_area": 1000}
            if x == area_end
            else {"start_area": x, "end_area": x + area_step}
            for x in range(area_start, area_end + area_step, area_step)
        ]
        data = {
            "time_stamp": self.crawling_start_time,
            "run_by": run_by,
            "finish_time_stamp": str(timestamp(tznow())),
            "total_statistics": total_statistics,
            "area_range": area_range,
        }

        folder_name = (
            f"{self.config['ENVIRONMENT']}/"
            f"{self.crawling_date.year}/"
            f"{self.crawling_date.month:02}/"
            f"{self.crawling_date.day:02}/"
            f"{str(self.crawling_start_time)}/"
            f"crawler-log"
        )

        file_name = f"{self.crawling_start_time}.json"

        self.s3_client.upload_json(
            folder_name=folder_name, file_name=file_name, data=data
        )

        logger.info(
            "Upload crawler log to s3",
            folder_name=folder_name,
            file_name=file_name,
        )
