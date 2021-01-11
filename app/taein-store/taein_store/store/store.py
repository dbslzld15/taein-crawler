import typing
import re
import json
import structlog
from crawler.aws_client import S3Client
from tanker.slack import SlackClient
from tanker.utils.datetime import tzfromtimestamp
from loan_model.models.taein.taein_statistics import TaeinStatistics
from loan_model.models.taein.taein_bid import TaeinBid
from loan_model.models.taein.taein_area_range import TaeinAreaRange
from loan_model.models.taein.taein_gugun import TaeinGugun
from loan_model.models.taein.taein_dong import TaeinDong
from loan_model.models.taein.taein_sido import TaeinSido
from crawler.taein_schema import (
    TaeinStatisticsResponse,
    TaeinBidResponse,
    TaeinBidData,
    SIDO_REGION_DICT,
)
from taein_store.db import create_session_factory
from taein_store.store.data import CrawlerLogResponse
from taein_store.store.exc import (
    TaeinStoreS3NotFound,
    TaeinStoreRegionNotFound,
)

logger = structlog.get_logger(__name__)


class TaeinStore(object):
    def __init__(self, config: typing.Dict[str, typing.Any]) -> None:
        super().__init__()
        self.config = config
        self.session_factory = create_session_factory(config)
        self.s3_client = S3Client(config)
        self.slack_client = SlackClient(
            config.get("SLACK_CHANNEL"), config.get("SLACK_API_TOKEN")
        )
        self.region_level_1 = self.config["REGION_REGEX_LEVEL_1"]
        self.region_level_2 = self.config["REGION_REGEX_LEVEL_2"]
        self.region_level_3 = self.config["REGION_REGEX_LEVEL_3"]
        self.completed_sido_statistics: typing.Dict[
            str, typing.List[int]
        ] = dict()
        self.completed_gugun_statistics: typing.Dict[
            str, typing.List[int]
        ] = dict()
        self.completed_sido_ids: typing.Dict[str, int] = dict()
        self.completed_gugun_ids: typing.Dict[str, int] = dict()

    def run(self, run_by: str) -> None:
        """
        로컬에서 실행 시 DB에 taein_area_range를 덤프해야 제대로 작동합니다
        """

        self.slack_client.send_info_slack(
            f"Store 시작합니다. ({self.config['ENVIRONMENT']}, {run_by})"
        )

        crawler_log_id = self.config["CRAWLER_LOG_ID"]

        self.check_area_range_valid_or_not()  # 크롤링한 area_range 맞는지 체크

        if crawler_log_id:
            self.fetch_received_log_folder()  # 수동 log id 폴더 저장
        else:
            self.fetch_latest_log_folder()  # 최신 log id 폴더 저장

        self.slack_client.send_info_slack(
            f"Store 종료합니다. ({self.config['ENVIRONMENT']}, {run_by})"
        )

    def check_area_range_valid_or_not(self) -> None:
        crawler_log = self.fetch_crawler_log()
        area_range_list = crawler_log.area_range_list
        session = self.session_factory()
        try:
            for area_range in area_range_list:
                session.query(TaeinAreaRange).filter(
                    TaeinAreaRange.start_area == area_range.start_area,
                    TaeinAreaRange.end_area == area_range.end_area,
                ).one()
        except Exception:
            raise
        finally:
            session.close()

    def fetch_received_log_folder(self) -> None:
        crawler_log_id = self.config["CRAWLER_LOG_ID"]
        crawler_date = tzfromtimestamp(float(crawler_log_id))
        log_id_prefix = (
            f"{self.config['ENVIRONMENT']}/"
            f"{crawler_date.year}/"
            f"{crawler_date.month}/"
            f"{crawler_date.day}/"
            f"{crawler_log_id}/"
        )
        self.fetch_sido_region_folder(log_id_prefix)

    def fetch_latest_log_folder(self) -> None:
        env_prefix = f"{self.config['ENVIRONMENT']}/"
        year_prefix = self.fetch_latest_folder(env_prefix)
        month_prefix = self.fetch_latest_folder(year_prefix)
        day_prefix = self.fetch_latest_folder(month_prefix)
        log_id_prefix = self.fetch_latest_folder(day_prefix)
        self.fetch_sido_region_folder(log_id_prefix)

    def fetch_latest_folder(self, base_prefix: str) -> str:
        date_list: typing.List[str] = list()
        for response in self.s3_client.get_objects(base_prefix, Delimiter="/"):
            prefixes = response.common_prefixes
            if not prefixes:
                raise TaeinStoreS3NotFound("not found date list")
            for date_prefix in prefixes:
                date = (
                    date_prefix["Prefix"]
                    .replace(base_prefix, "")
                    .replace("/", "")
                    .strip()
                )
                date_list.append(date)
            date_list.sort()
        base_prefix += date_list[-1] + "/"

        return base_prefix

    def fetch_sido_region_folder(self, log_id_prefix: str) -> None:
        data_prefix = log_id_prefix + "data/"
        sido_check: bool = False
        for response in self.s3_client.get_objects(data_prefix, Delimiter="/"):
            prefixes = response.common_prefixes
            if not prefixes:
                raise TaeinStoreS3NotFound("not found sido region list")
            for sido_prefix in prefixes:
                sido_name = (
                    sido_prefix["Prefix"]
                    .replace(data_prefix, "")
                    .replace("/", "")
                    .strip()
                )
                if re.search(self.region_level_1, sido_name):
                    sido_check = True
                    self.fetch_gugun_region_folder(sido_prefix["Prefix"])

        if not sido_check:
            raise TaeinStoreRegionNotFound(
                f"not found sido({self.region_level_1})"
            )

    def fetch_gugun_region_folder(self, sido_prefix: str) -> None:
        """
        인천 남구와 인천 미추홀구는 같은 지역입니다. 태인경매 홈페이지에서는 해당 두 지역에 대한
        데이터를 모두 가지고 있는데 남구의 경우 데이터가 부정확하며 미추홀구로 검색해야 데이터가
        정확합니다. 따라서 인천 남구는 스킵시켜줍니다.
        """
        gugun_check: bool = False
        for response in self.s3_client.get_objects(sido_prefix, Delimiter="/"):
            prefixes = response.common_prefixes
            if not prefixes:
                raise TaeinStoreS3NotFound("not found gugun region list")
            for gugun_prefix in prefixes:
                gugun_name = (
                    gugun_prefix["Prefix"]
                    .replace(sido_prefix, "")
                    .replace("/", "")
                    .strip()
                )
                if "인천" in sido_prefix and gugun_name == "남구":
                    continue
                if re.search(self.region_level_2, gugun_name):
                    gugun_check = True
                    self.fetch_dong_region_folder(gugun_prefix["Prefix"])

        if not gugun_check:
            raise TaeinStoreRegionNotFound(
                f"not found gugun({self.region_level_2})"
            )

    def fetch_dong_region_folder(self, gugun_prefix: str) -> None:
        dong_check: bool = False
        for response in self.s3_client.get_objects(
            gugun_prefix, Delimiter="/"
        ):
            prefixes = response.common_prefixes
            if not prefixes:
                raise TaeinStoreS3NotFound("not found dong region list")
            for dong_prefix in prefixes:
                dong_name = (
                    dong_prefix["Prefix"]
                    .replace(gugun_prefix, "")
                    .replace("/", "")
                    .strip()
                )
                if re.search(self.region_level_3, dong_name):
                    dong_check = True
                    self.fetch_mulgun_kind_folder(dong_prefix["Prefix"])

        if not dong_check:
            raise TaeinStoreRegionNotFound(
                f"not found dong({self.region_level_3})"
            )

    def fetch_mulgun_kind_folder(self, dong_prefix: str) -> None:
        for response in self.s3_client.get_objects(dong_prefix, Delimiter="/"):
            prefixes = response.common_prefixes
            if not prefixes:
                raise TaeinStoreS3NotFound("not found main using list")
            for mulgun_kind_prefix in prefixes:
                self.fetch_statistics_folder(mulgun_kind_prefix["Prefix"])

    def fetch_statistics_folder(self, mulgun_kind_prefix: str) -> None:
        for response in self.s3_client.get_objects(
            mulgun_kind_prefix, Delimiter="/"
        ):
            db_dong_id: typing.Optional[int] = None
            contents = response.contents
            prefixes = response.common_prefixes
            if not contents:
                raise TaeinStoreS3NotFound("not found statistics data")
            for content in contents:
                file_prefix = content["Key"]
                s3_response = self.s3_client.get_object(file_prefix)
                statistics_data = s3_response.body.read().decode("utf-8")
                statistics = TaeinStatisticsResponse.from_html(statistics_data)

                sido_name = statistics.sido_name
                gugun_name = statistics.gugun_name
                dong_name = statistics.dong_name

                db_area_range_id = self.get_area_range(
                    statistics.building_start_area,
                    statistics.building_end_area,
                )

                try:
                    self.completed_sido_statistics[sido_name]
                except KeyError:
                    self.completed_sido_statistics[sido_name] = list()

                if (
                    db_area_range_id
                    not in self.completed_sido_statistics[sido_name]
                ):
                    db_sido_id = self.store_sido_region(sido_name)
                    self.store_statistics_data(
                        statistics, db_area_range_id, db_sido_id=db_sido_id
                    )
                    # sido id 캐싱
                    self.completed_sido_ids.update({sido_name: db_sido_id})
                    # 전용 면적에 대한 시도 데이터 저장완료 캐싱
                    self.completed_sido_statistics[sido_name].append(
                        db_area_range_id
                    )

                try:
                    self.completed_gugun_statistics[sido_name + gugun_name]
                except KeyError:
                    self.completed_gugun_statistics[
                        sido_name + gugun_name
                    ] = list()

                if (
                    db_area_range_id
                    not in self.completed_gugun_statistics[
                        sido_name + gugun_name
                    ]
                ):
                    db_sido_id = self.completed_sido_ids[sido_name]
                    db_gugun_id = self.store_gugun_region(
                        gugun_name, db_sido_id
                    )
                    self.store_statistics_data(
                        statistics, db_area_range_id, db_gugun_id=db_gugun_id
                    )
                    # gugun id 캐싱
                    self.completed_gugun_ids.update(
                        {sido_name + gugun_name: db_gugun_id}
                    )
                    # 전용 면적에 대한 구군 데이터 저장완료 캐싱
                    self.completed_gugun_statistics[
                        sido_name + gugun_name
                    ].append(db_area_range_id)

                # 읍,면,동 통계 저장
                db_gugun_id = self.completed_gugun_ids[sido_name + gugun_name]
                db_dong_id = self.store_dong_region(dong_name, db_gugun_id)
                self.store_statistics_data(
                    statistics, db_area_range_id, db_dong_id=db_dong_id
                )

            if prefixes:
                for bid_prefix in prefixes:
                    self.fetch_bid_folder(bid_prefix["Prefix"], db_dong_id)

    def fetch_bid_folder(self, data_type_prefix: str, db_dong_id: int) -> None:
        for response in self.s3_client.get_objects(
            data_type_prefix, Delimiter="/"
        ):
            contents = response.contents
            if not contents:
                raise TaeinStoreS3NotFound("not found bid data")
            for content in contents:
                file_prefix = content["Key"]
                s3_response = self.s3_client.get_object(file_prefix)
                bid_data = s3_response.body.read().decode("utf-8")
                bid = TaeinBidResponse.from_html(bid_data)
                self.store_bid_data(bid.taein_bid_list, db_dong_id)

    def fetch_crawler_log(self) -> CrawlerLogResponse:
        log_prefix = self.fetch_crawler_log_path()
        response = self.s3_client.get_object(log_prefix)
        json_log = json.loads(response.body.read())
        return CrawlerLogResponse.from_json(json_log)

    def fetch_crawler_log_path(self) -> str:  # 최신 로그 폴더 경로
        env_prefix = f"{self.config['ENVIRONMENT']}/"
        year_list: typing.List[str] = []
        month_list: typing.List[str] = []
        day_list: typing.List[str] = []
        time_stamp_list: typing.List[str] = []
        log_id_list: typing.List[str] = []

        for response in self.s3_client.get_objects(env_prefix, Delimiter="/"):
            prefixes = response.common_prefixes
            for year_prefix in prefixes:
                year = (
                    year_prefix["Prefix"]
                    .replace(env_prefix, "")
                    .replace("/", "")
                    .strip()
                )
                year_list.append(year)
            year_list.sort()
        year_prefix = env_prefix + year_list[-1] + "/"

        for response in self.s3_client.get_objects(year_prefix, Delimiter="/"):
            prefixes = response.common_prefixes
            for month_prefix in prefixes:
                month = (
                    month_prefix["Prefix"]
                    .replace(year_prefix, "")
                    .replace("/", "")
                    .strip()
                )
                month_list.append(month)
            month_list.sort()
        month_prefix = year_prefix + month_list[-1] + "/"

        for response in self.s3_client.get_objects(
            month_prefix, Delimiter="/"
        ):
            prefixes = response.common_prefixes

            for day_prefix in prefixes:
                day = (
                    day_prefix["Prefix"]
                    .replace(month_prefix, "")
                    .replace("/", "")
                    .strip()
                )
                day_list.append(day)
            day_list.sort()
        day_prefix = month_prefix + day_list[-1] + "/"

        for response in self.s3_client.get_objects(day_prefix, Delimiter="/"):
            prefixes = response.common_prefixes
            for time_stamp_prefix in prefixes:
                time_stamp = (
                    time_stamp_prefix["Prefix"]
                    .replace(day_prefix, "")
                    .replace("/", "")
                    .strip()
                )
                time_stamp_list.append(time_stamp)
            time_stamp_list.sort()

        time_stamp_prefix = day_prefix + time_stamp_list[-1] + "/"

        log_prefix = f"{time_stamp_prefix}crawler-log/"

        for response in self.s3_client.get_objects(log_prefix):
            for content in response.contents:
                log_id = content["Key"].split("/")[-1].replace(".json", "")
                log_id_list.append(log_id)
            log_id_list.sort()

        log_prefix += log_id_list[-1] + ".json"

        return log_prefix

    def get_area_range(self, start_area: str, end_area: str) -> int:
        if start_area == "최소":
            start_area = 0
        if end_area == "최대":
            end_area = 1000

        session = self.session_factory()
        try:
            db_area_range = (
                session.query(TaeinAreaRange)
                .filter(
                    TaeinAreaRange.start_area == start_area,
                    TaeinAreaRange.end_area == end_area,
                )
                .one()
            )
            db_area_range_id = db_area_range.id
            session.commit()
        except Exception:
            raise
        finally:
            session.close()

        return db_area_range_id

    def store_sido_region(self, sido_name: str) -> int:
        session = self.session_factory()
        beautified_name = SIDO_REGION_DICT[sido_name]
        try:
            db_sido = TaeinSido.create_or_update(
                session, sido_name, beautified_name
            )
            db_sido_id = db_sido.id
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

        return db_sido_id

    def store_gugun_region(self, gugun_name: str, db_sido_id: int) -> int:
        session = self.session_factory()
        try:
            db_gugun = TaeinGugun.create_or_update(
                session, gugun_name, db_sido_id
            )
            db_gugun_id = db_gugun.id
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

        return db_gugun_id

    def store_dong_region(self, dong_name: str, db_gugun_id: int) -> int:
        session = self.session_factory()
        try:
            db_dong = TaeinDong.create_or_update(
                session, dong_name, db_gugun_id
            )
            db_dong_id = db_dong.id
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

        return db_dong_id

    def store_statistics_data(
        self,
        data: TaeinStatisticsResponse,
        db_area_range_id: int,
        *,
        db_sido_id: typing.Optional[int] = None,
        db_gugun_id: typing.Optional[int] = None,
        db_dong_id: typing.Optional[int] = None,
    ) -> None:
        session = self.session_factory()
        # Store sido statistics
        if db_sido_id:
            try:
                TaeinStatistics.create_or_update(
                    session,
                    data.start_date_str,
                    data.start_date,
                    data.end_date_str,
                    data.end_date,
                    str(data.sido_year_avg_price_rate),
                    data.sido_year_avg_price_rate,
                    str(data.sido_year_avg_bid_rate),
                    data.sido_year_avg_bid_rate,
                    data.sido_year_bid_count,
                    str(data.sido_six_month_avg_price_rate),
                    data.sido_six_month_avg_price_rate,
                    str(data.sido_six_month_avg_bid_rate),
                    data.sido_six_month_avg_bid_rate,
                    data.sido_six_month_bid_count,
                    db_sido_id,
                    db_gugun_id,
                    db_dong_id,
                    db_area_range_id,
                )
                session.commit()
            except Exception:
                session.rollback()
                raise
            finally:
                session.close()
            logger.info("Store Sido Statistics", sido=data.sido_name)

        # Store gugun statistics
        elif db_gugun_id:
            try:
                TaeinStatistics.create_or_update(
                    session,
                    data.start_date_str,
                    data.start_date,
                    data.end_date_str,
                    data.end_date,
                    str(data.gugun_year_avg_price_rate),
                    data.gugun_year_avg_price_rate,
                    str(data.gugun_year_avg_bid_rate),
                    data.gugun_year_avg_bid_rate,
                    data.gugun_year_bid_count,
                    str(data.gugun_six_month_avg_price_rate),
                    data.gugun_six_month_avg_price_rate,
                    str(data.gugun_six_month_avg_bid_rate),
                    data.gugun_six_month_avg_bid_rate,
                    data.gugun_six_month_bid_count,
                    db_sido_id,
                    db_gugun_id,
                    db_dong_id,
                    db_area_range_id,
                )
                session.commit()
            except Exception:
                session.rollback()
                raise
            finally:
                session.close()
            logger.info(
                "Store Gugun Statistics",
                sido=data.sido_name,
                gugun=data.gugun_name,
            )

        # Store dong statistics
        elif db_dong_id:
            try:
                TaeinStatistics.create_or_update(
                    session,
                    data.start_date_str,
                    data.start_date,
                    data.end_date_str,
                    data.end_date,
                    str(data.dong_year_avg_price_rate),
                    data.dong_year_avg_price_rate,
                    str(data.dong_year_avg_bid_rate),
                    data.dong_year_avg_bid_rate,
                    data.dong_year_bid_count,
                    str(data.dong_six_month_avg_price_rate),
                    data.dong_six_month_avg_price_rate,
                    str(data.dong_six_month_avg_bid_rate),
                    data.dong_six_month_avg_bid_rate,
                    data.dong_six_month_bid_count,
                    db_sido_id,
                    db_gugun_id,
                    db_dong_id,
                    db_area_range_id,
                )
                session.commit()
            except Exception:
                session.rollback()
                raise
            finally:
                session.close()
            logger.info(
                "Store Dong Statistics",
                sido=data.sido_name,
                gugun=data.gugun_name,
                dong=data.dong_name,
            )
        else:
            session.close()
            raise

    def store_bid_data(
        self, bid_list: typing.List[TaeinBidData], db_dong_id: int
    ) -> None:
        for bid in bid_list:
            session = self.session_factory()
            try:
                TaeinBid.create_or_update(
                    session,
                    bid.bid_date_str,
                    bid.bid_date,
                    bid.bid_event_number,
                    bid.address,
                    bid.bid_judged_price,
                    bid.bid_success_price,
                    bid.average_bid_rate_str,
                    bid.average_bid_rate,
                    bid.bidder_count,
                    bid.bid_kind,
                    db_dong_id,
                )
                session.commit()
            except Exception:
                session.rollback()
                raise
            finally:
                session.close()
            logger.info("Store Bid statistics", bid=bid.address)
