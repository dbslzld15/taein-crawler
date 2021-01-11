import typing

import attr


@attr.s
class CrawlerStatistics(object):
    #: 수집한 데이터 갯수
    statistics_count: int = attr.ib(default=0)
    #: 낙찰사례 사례 갯수
    bids_count: int = attr.ib(default=0)

    class CrawlerStatisticsData(typing.Dict):
        statistics_count: int
        bids_count: int

    @classmethod
    def from_json(cls, data: CrawlerStatisticsData) -> "CrawlerStatistics":
        return cls(
            statistics_count=data["statistics_count"],
            bids_count=data["bids_count"],
        )


def slack_failure_percentage_statistics(
    total_statistics: CrawlerStatistics, failure_statistics: CrawlerStatistics,
) -> typing.Dict[str, typing.Any]:
    total_statistics_dict = attr.asdict(total_statistics)
    failure_statistics_dict = attr.asdict(failure_statistics)

    keys = tuple(total_statistics_dict.keys())

    result = dict()
    for key in keys:
        total_value = total_statistics_dict[key]
        failure_value = failure_statistics_dict[key]

        try:
            result[key] = (
                f"total: {total_value}\n"
                f"fail: {failure_value}\n"
                f"{100 *  failure_value / total_value}%"
            )
        except ZeroDivisionError:
            result[key] = (
                f"total: {total_value}\n"
                f"fail: {failure_value}\n"
            )

    return result
