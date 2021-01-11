import typing

import sqlalchemy as sa
from loan_model.models.base import Model as LoanModel
from sqlalchemy import orm


def create_session_factory(
    config: typing.Dict[str, typing.Any]
) -> orm.session:
    db_uri = config["SQLALCHEMY_DATABASE_URI"]
    db_engine = sa.create_engine(db_uri)
    session_factory = orm.sessionmaker(bind=db_engine)
    return session_factory


def init_loan_db_schema(session: orm.Session):
    LoanModel.metadata.drop_all(session.bind)
    LoanModel.metadata.create_all(session.bind)
