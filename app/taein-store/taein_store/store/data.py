import typing
import attr


@attr.s
class CrawlerAreaRange(object):
    start_area: int = attr.ib()
    end_area: int = attr.ib()

    @classmethod
    def from_json(
        cls, data: typing.Dict[str, typing.Any]
    ) -> "CrawlerAreaRange":
        return cls(start_area=data["start_area"], end_area=data["end_area"])


@attr.s(frozen=True)
class CrawlerStatistics(object):
    #: 수집된 통계 페이지 갯수
    statistics_count: int = attr.ib(default=0)
    #: 수집된 낙찰사례 갯수
    bids_count: int = attr.ib(default=0)

    @classmethod
    def from_json(
        cls, data: typing.Dict[str, typing.Any]
    ) -> "CrawlerStatistics":
        return cls(
            statistics_count=data["statistics_count"],
            bids_count=data["bids_count"],
        )


@attr.s(frozen=True)
class CrawlerLogResponse(object):
    time_stamp: float = attr.ib()
    run_by: str = attr.ib()
    finish_time_stamp: float = attr.ib()
    total_statistics: CrawlerStatistics = attr.ib()
    area_range_list: typing.List[CrawlerAreaRange] = attr.ib()

    @classmethod
    def from_json(
        cls, data: typing.Dict[str, typing.Any]
    ) -> "CrawlerLogResponse":
        return cls(
            time_stamp=float(data["time_stamp"]),
            run_by=data["run_by"],
            finish_time_stamp=float(data["finish_time_stamp"]),
            total_statistics=CrawlerStatistics.from_json(
                data["total_statistics"]
            ),
            area_range_list=[
                CrawlerAreaRange.from_json(x) for x in data["area_range"]
            ],
        )
