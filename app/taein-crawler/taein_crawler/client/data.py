import typing
from abc import abstractmethod, ABCMeta
import re
import attr
import bs4


class TaeinData(metaclass=ABCMeta):
    @abstractmethod
    def to_html(self) -> str:
        pass


@attr.s
class TaeinGugun(TaeinData):
    gugun_name: str = attr.ib()
    dong_list: typing.List[str] = attr.ib()
    raw_data: typing.Dict[str, str] = attr.ib()

    @classmethod
    def from_html(cls, data: typing.Dict[str, str]) -> "TaeinGugun":
        dong_list = list()
        gugun_name = data["gugun"]
        raw_dong_list = data["dong_list"].split(",")
        for raw_dong in raw_dong_list:
            dong_name = raw_dong.replace("'", "")
            if dong_name == "전체":
                continue
            dong_list.append(dong_name)
        return cls(
            gugun_name=gugun_name,
            dong_list=dong_list,
            raw_data=data,
        )

    def to_html(self) -> typing.Dict[str, str]:
        return self.raw_data


@attr.s
class TaeinSido(TaeinData):
    sido_name: str = attr.ib()
    taein_gugun: TaeinGugun = attr.ib()
    raw_data: typing.Dict[str, str] = attr.ib()

    @classmethod
    def from_html(cls, data: typing.Dict[str, str]) -> "TaeinSido":
        sido_name = data["sido_gugun"][:2]
        gugun_name = data["sido_gugun"][2:]
        region_dict = {"gugun": gugun_name, "dong_list": data["dong_list"]}
        return cls(
            sido_name=sido_name,
            taein_gugun=TaeinGugun.from_html(region_dict),
            raw_data=data,
        )

    def to_html(self) -> typing.Dict[str, str]:
        return self.raw_data


@attr.s
class TaeinRegion(TaeinData):
    taein_sido_list: typing.List[TaeinSido] = attr.ib()
    raw_data: str = attr.ib()

    @classmethod
    def from_html(cls, data: str) -> "TaeinRegion":
        taein_sido_list = list()
        sido_gugun_list = re.findall("case '(.+)': raddr3", data)
        dong_list = re.findall(r"raddr3 = new Array\((.+)\)", data)

        for i in range(len(sido_gugun_list)):
            try:
                if (
                    sido_gugun_list[i] == sido_gugun_list[i + 1]
                    or sido_gugun_list[i] in sido_gugun_list[i + 1]
                    or len(sido_gugun_list) == 2
                ):
                    continue
            except IndexError:
                pass

            region_dict = {
                "sido_gugun": sido_gugun_list[i],
                "dong_list": dong_list[i],
            }
            taein_sido_list.append(TaeinSido.from_html(region_dict))

        return cls(
            taein_sido_list=taein_sido_list,
            raw_data=data,
        )

    def to_html(self) -> str:
        return self.raw_data


@attr.s
class TaeinMulgunKind(TaeinData):
    mulgun_kind_dict: typing.Dict[str, str] = attr.ib()
    raw_data: str = attr.ib()

    @classmethod
    def from_html(cls, data: str) -> "TaeinMulgunKind":
        mulgun_kind_dict = dict()
        soup = bs4.BeautifulSoup(data, "lxml")
        mulgun_kind_select = soup.find("select", attrs={"name": "mulgun_kind"})
        mulgun_kind_options = mulgun_kind_select.find_all("option")

        for mulgun_kind in mulgun_kind_options:
            if "전체" in mulgun_kind.text:
                continue
            mulgun_kind_value = re.findall(
                r"value=\"(.+)\"", str(mulgun_kind)
            )[0]
            mulgun_kind_text = re.compile("[가-힣]+").findall(mulgun_kind.text)[
                0
            ]
            mulgun_kind_dict.update({mulgun_kind_text: mulgun_kind_value})

        return cls(
            mulgun_kind_dict=mulgun_kind_dict,
            raw_data=data,
        )

    def to_html(self) -> str:
        return self.raw_data


@attr.s
class TaeinStatisticsResponse(TaeinData):
    bid_count: int = attr.ib()
    bid_total_page: int = attr.ib()
    dong_statistics_exist: bool = attr.ib()
    raw_data: str = attr.ib()

    @classmethod
    def from_html(cls, data: str) -> "TaeinStatisticsResponse":
        soup = bs4.BeautifulSoup(data, "lxml")
        statistics_div = soup.find(
            "div", attrs={"class": "stdata_area prt_area"}
        )
        tbody = statistics_div.find("tbody")
        tr_list = tbody.find_all("tr")
        td_list = tr_list[0].find_all("td")
        bid_count_td = td_list[1]
        bid_count = re.findall(r"\d+", bid_count_td.text)
        if bid_count:
            bid_count = int(bid_count[0])
        else:
            bid_count = 0

        if bid_count % 10 == 0:
            bid_total_page = bid_count // 10
        else:
            bid_total_page = bid_count // 10 + 1

        statistics_table = soup.find("table", attrs={"class": "stat_LIST"})
        table_tr_list = statistics_table.find_all("tr")
        header_tr = table_tr_list[0]
        header_th_list = header_tr.find_all("th")

        dong_statistics_exist = False
        if len(header_th_list) >= 4:
            dong_statistics_exist = True

        return cls(
            bid_count=bid_count,
            bid_total_page=bid_total_page,
            dong_statistics_exist=dong_statistics_exist,
            raw_data=str(soup),
        )

    def to_html(self) -> str:
        return self.raw_data


@attr.s
class TaeinBidResponse(TaeinData):
    raw_data: str = attr.ib()

    @classmethod
    def from_html(cls, data: str) -> "TaeinBidResponse":
        soup = bs4.BeautifulSoup(data, "lxml")
        return cls(raw_data=str(soup))

    def to_html(self) -> str:
        return self.raw_data
