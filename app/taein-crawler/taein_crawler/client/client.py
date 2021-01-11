import functools
import json
import random
import time
import typing
from datetime import datetime

import requests
from requests_toolbelt.sessions import BaseUrlSession
from taein_crawler.client.exc import (
    TaeinClientResponseError,
)
from tanker.utils.requests import apply_proxy
from tanker.utils.retryer import Retryer
from tanker.utils.retryer.strategy import ExponentialModulusBackoffStrategy

from .data import (
    TaeinRegion,
    TaeinStatisticsResponse,
    TaeinBidResponse,
    TaeinMulgunKind,
)

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5)"
    " AppleWebKit/537.36 (KHTML, like Gecko)"
    " Chrome/83.0.4103.116 Safari/537.36"
)


class TaeinClient(object):
    def __init__(
        self,
        *,
        client_delay: typing.Optional[str] = None,
        proxy: typing.Optional[str] = None
    ) -> None:
        super().__init__()
        self.client_delay = client_delay

        # Header Settings
        self.session = BaseUrlSession("https://www.taein.co.kr/")
        self.session.headers.update({"User-Agent": USER_AGENT})

        if proxy:
            apply_proxy(self.session, proxy)

        self.retryer = Retryer(
            strategy_factory=(
                ExponentialModulusBackoffStrategy.create_factory(2, 10)
            ),
            should_retry=lambda e: isinstance(
                e, (requests.exceptions.ConnectionError,)
            ),
            default_max_trials=3,
        )

    def _handle_json_response(
        self, r: requests.Response
    ) -> typing.Dict[str, typing.Any]:
        r.raise_for_status()

        try:
            data = r.json()

            return data
        except (json.JSONDecodeError, ValueError):
            raise TaeinClientResponseError(r.status_code, r.text)

    def _handle_text_response(self, r: requests.Response) -> str:
        r.raise_for_status()
        if self.client_delay:
            time.sleep(float(self.client_delay))
        try:
            r.json()
        except (json.JSONDecodeError, ValueError):
            r.encoding = "euc-kr"
            return r.text
        else:
            raise TaeinClientResponseError(r.status_code, r.text)

    def fetch_main_page(self) -> str:
        self.retryer.run(functools.partial(self.session.get, ""))

        response = self._handle_text_response(
            self.retryer.run(functools.partial(self.session.get, "main1.html"))
        )

        return response

    def fetch_login_page(self) -> str:
        params = {"v": datetime.today().strftime("%Y%m%d%H%M%S")}

        response = self._handle_text_response(
            self.retryer.run(
                functools.partial(
                    self.session.get, "member/index_login.php", params=params
                )
            )
        )

        return response

    def login(
        self,
        login_id: str,
        login_pw: str
    ) -> str:
        data = {
            "login_id": login_id,
            "save_id": "N",
            "login_password": login_pw,
            "save_pw": "N",
            "x": random.randint(1, 18),
            "y": random.randint(1, 18),
        }

        response = self._handle_text_response(
            self.retryer.run(
                functools.partial(
                    self.session.post, "member/login_end.php", data=data
                )
            )
        )

        return response

    def logout(self) -> str:

        response = self._handle_text_response(
            self.retryer.run(
                functools.partial(
                    self.session.get, "member/logout.php"
                )
            )
        )

        return response

    def fetch_region_list(self) -> TaeinRegion:

        response = self._handle_text_response(
            self.retryer.run(
                functools.partial(
                    self.session.get, "common/js/address_3rd_161115.js"
                )
            )
        )

        return TaeinRegion.from_html(response)

    def fetch_mulgun_kind_list(
        self, start_date: datetime, end_date: datetime
    ) -> TaeinMulgunKind:

        start_year = start_date.year
        start_month = f"{start_date.month:02}"
        start_day = f"{start_date.day:02}"
        end_year = end_date.year
        end_month = f"{end_date.month:02}"
        end_day = f"{end_date.day:02}"

        params = {
            "takeQuery": "exe",
            "vcase": "1",
            "var_service": "",
            "var_kind": "",
            "rdo_local": "1",
            "addr1": "서울".encode("euc-kr"),
            "addr2": "강남구".encode("euc-kr"),
            "addr3": "개포동".encode("euc-kr"),
            "bupwon_gae": "",
            "mulgun_kind": "",
            "low_gam": "",
            "high_gam": "",
            "low_yuchal": "",
            "high_yuchal": "",
            "low_bdarea": "최소".encode("euc-kr"),
            "high_bdarea": "최대".encode("euc-kr"),
            "low_daejiarea": "최소".encode("euc-kr"),
            "high_daejiarea": "최대".encode("euc-kr"),
            "start_year": start_year,
            "start_month": start_month,
            "start_day": start_day,
            "end_year": end_year,
            "end_month": end_month,
            "end_day": end_day,
            "txt_local": "",
            "rtnpage": "/auction/statistics/goods_stat.php",
        }

        response = self._handle_text_response(
            self.retryer.run(
                functools.partial(
                    self.session.get,
                    "auction/statistics/goods_stat.php",
                    params=params,
                )
            )
        )

        return TaeinMulgunKind.from_html(response)

    def fetch_statistics_page(
        self,
        sido: str,
        gugun: str,
        dong: str,
        start_area: str,
        end_area: str,
        start_date: datetime,
        end_date: datetime,
        mulgun_kind_value: str
    ) -> TaeinStatisticsResponse:

        start_year = start_date.year
        start_month = f"{start_date.month:02}"
        start_day = f"{start_date.day:02}"
        end_year = end_date.year
        end_month = f"{end_date.month:02}"
        end_day = f"{end_date.day:02}"

        params = {
            "takeQuery": "exe",
            "vcase": "1",
            "var_service": "",
            "var_kind": "",
            "rdo_local": "1",
            "addr1": sido.encode("euc-kr"),
            "addr2": gugun.encode("euc-kr"),
            "addr3": dong.encode("euc-kr"),
            "bupwon_gae": "",
            "mulgun_kind": mulgun_kind_value,
            "low_gam": "",
            "high_gam": "",
            "low_yuchal": "",
            "high_yuchal": "",
            "low_bdarea": start_area
            if start_area.isdigit()
            else start_area.encode("euc-kr"),
            "high_bdarea": end_area
            if end_area.isdigit()
            else end_area.encode("euc-kr"),
            "low_daejiarea": "최소".encode("euc-kr"),
            "high_daejiarea": "최대".encode("euc-kr"),
            "start_year": start_year,
            "start_month": start_month,
            "start_day": start_day,
            "end_year": end_year,
            "end_month": end_month,
            "end_day": end_day,
            "txt_local": "",
            "rtnpage": "/auction/statistics/goods_stat.php",
        }

        response = self._handle_text_response(
            self.retryer.run(
                functools.partial(
                    self.session.get,
                    "auction/statistics/goods_stat.php",
                    params=params,
                )
            )
        )

        return TaeinStatisticsResponse.from_html(response)

    def fetch_bid_list_page(
        self,
        sido: str,
        gugun: str,
        dong: str,
        start_date: datetime,
        end_date: datetime,
        mulgun_kind: str,
        bid_count: int,
        page_index: int
    ) -> TaeinBidResponse:

        start_year = start_date.year
        start_month = f"{start_date.month:02}"
        start_day = f"{start_date.day:02}"
        end_year = end_date.year
        end_month = f"{end_date.month:02}"
        end_day = f"{end_date.day:02}"
        data = {
            "SITE_NAME": "TAEIN",
            "bubcode": "101/102/103/104/105/106/"
            "302/303/304/202/301/201/"
            "305/107/306/601/602/D01/"
            "B01/B05/B02/B04/B03/B06/"
            "701/702/708/703/704/707/"
            "705/706/709/801/803/802/"
            "804/805/C01/C03/C04/C02/"
            "401/402/404/405/406/403/"
            "A02/A03/A04/A01/501/503/"
            "504/502/505/901/603/",
            "bupwon_num": "",
            "bupwon_gae": "",
            "local_num": "",
            "rdo_local": "1",
            "addr1": sido.encode("utf-8"),
            "addr2": gugun.encode("utf-8"),
            "addr3": dong.encode("utf-8"),
            "start_date": f"{start_year}{start_month}{start_day}",
            "end_date": f"{end_year}{end_month}{end_day}",
            "low_gam": "",
            "high_gam": "",
            "low_bdarea": "최소".encode("utf-8"),
            "high_bdarea": "최대".encode("utf-8"),
            "low_daejiarea": "최소".encode("utf-8"),
            "high_daejiarea": "최대".encode("utf-8"),
            "high_yuchal": "",
            "low_yuchal": "",
            "mulgun_kind": mulgun_kind
            if mulgun_kind.isdigit()
            else mulgun_kind.encode("utf-8"),
            "var_service": "",
            "var_kind": "",
            "takeQuery": "exe",
            "vcase": "1",
            "order_condition": "",
            "sun_imchain": "",
            "sun_junsekwon": "",
            "maesusingo_yn": "",
            "m_jibun_yn": "",
            "yuchikwon_yn": "",
            "m_jisangkwon_yn": "",
            "m_gijikwon_yn": "",
            "sun_jisangkwon": "",
            "sun_gadunggi": "",
            "total": str(bid_count),
            "block": str(page_index),
            "start": "",
            "next": "",
            "sel_ydbox_no": "0",
        }

        response = self._handle_text_response(
            self.retryer.run(
                functools.partial(
                    self.session.post,
                    "auction/statistics/include/dataQuery.php",
                    data=data,
                )
            )
        )

        return TaeinBidResponse.from_html(response)
