"""
config
======

"""
import tanker.config
import typing
from tanker.config import fields

SCHEMA = {
    #: Running environment
    "ENVIRONMENT": fields.OneOfField(
        {"local", "test", "development", "production", }, default="local",
    ),
    #: Debug
    "DEBUG": fields.BooleanField(optional=True),
    #: Running environment
    "PROXY_HOST_LIST": fields.CommaSeparatedStringField(optional=False),
    #: AWS sepecific access key id value
    "AWS_ACCESS_KEY_ID": fields.StringField(optional=True),
    #: AWS sepecific secret access key value
    "AWS_SECRET_ACCESS_KEY": fields.StringField(optional=True),
    #: AWS sepecific region name value
    "AWS_REGION_NAME": fields.StringField(optional=True),
    #: AWS sepecific endpoint url value
    "AWS_ENDPOINT_URL": fields.StringField(optional=True),
    #: AWS sepecific endpoint url value
    "AWS_S3_BUCKET_NAME": fields.StringField(optional=True),
    #: Slack Info
    "SLACK_API_TOKEN": fields.StringField(optional=True),
    "SLACK_CHANNEL": fields.StringField(optional=True),
    # taein login id and password
    "LOGIN_ID": fields.StringField(optional=False),
    "LOGIN_PW": fields.StringField(optional=False),
    # 시, 도 지역
    'REGION_REGEX_LEVEL_1': fields.StringField(optional=True, default="서울"),
    # 시, 군, 구 지역
    'REGION_REGEX_LEVEL_2': fields.StringField(optional=True, default="강남구"),
    # 동, 읍, 면 지역
    'REGION_REGEX_LEVEL_3': fields.StringField(optional=True, default="개포동"),
    # 건물 면적 설정 범위
    "BUILDING_AREA_START": fields.IntegerField(optional=True, default=0),
    "BUILDING_AREA_END": fields.IntegerField(optional=True, default=400),
    "BUILDING_AREA_STEP": fields.IntegerField(optional=True, default=20),
    #: 물건 종류
    "MULGUN_KIND": fields.StringField(optional=True, default="아파트"),
    #: Sentry DSN
    'SENTRY_DSN': fields.StringField(optional=True),
    # 요청 딜레이
    "CLIENT_DELAY": fields.StringField(optional=False),
}


def load() -> typing.Dict[str, typing.Union[object, str]]:
    config = tanker.config.load_from_env(prefix="CRAWLER_", schema=SCHEMA)
    config.setdefault("DEBUG", config["ENVIRONMENT"] in {"local", "test"})
    return config
