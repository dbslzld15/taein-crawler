import code
import os
import typing

import attr
import click
import psutil
import sentry_sdk
import structlog
from apscheduler.schedulers.background import BackgroundScheduler
from crawler.aws_client import CloudWatchClient
from dotenv import load_dotenv, find_dotenv
from tanker.utils.logging import setup_logging
import taein_store.config
from taein_store.store import TaeinStore

logger = structlog.get_logger(__name__)


try:
    if not os.path.exists("./.env"):
        raise ValueError
    load_dotenv(find_dotenv(".env"), override=False, encoding="utf-8")
except (IOError, ValueError):
    load_dotenv(find_dotenv(".env.sample"), override=False)


@attr.s
class Context(object):
    config: typing.Dict[str, typing.Any] = attr.ib()


def init_runner(context: Context, run_by: str) -> typing.Callable:
    setup_logging(context.config["DEBUG"])

    sentry_sdk.init(dsn=context.config.get("SENTRY_DSN"))

    def runner() -> None:
        store = TaeinStore(context.config)
        store.run(run_by)

    return runner


@click.group()
@click.pass_context
def cli(ctx: typing.Any) -> None:
    ctx.obj = {}
    config = taein_store.config.load()

    ctx.obj["context"] = Context(config=config,)


@cli.command()
@click.option("--no-ipython", "no_ipython", default=False, is_flag=True)
@click.pass_context
def shell(ctx: typing.Any, no_ipython: typing.Any) -> None:
    """
    Run a Python REPL shell with some useful contextual values.

    """
    context: Context = ctx.obj["context"]

    variables = dict(context=context,)

    banner = (
        "vesta Python Interactive Shell (REPL)\n"
        "===========================================\n"
        "\n"
        "List of useful variables.\n"
        "\n"
        "*   context: CLI context.\n"
        "\n"
        "(InteractiveConsole)"
    )
    try:
        import IPython
    except ImportError:
        IPython = None
    if no_ipython or IPython is None:
        code.interact(banner=banner, local=variables)
    else:
        IPython.embed(user_ns=variables, banner1=banner)


@cli.command()
@click.pass_context
def run(ctx: typing.Any) -> None:
    context: Context = ctx.obj["context"]

    runner = init_runner(context, "DEVELOPER")

    runner()


# scheduled tasks로 돌릴 때 사용하는 함수이고, cloudwatch 로그를 찍습니다.
@cli.command()
@click.pass_context
def run_scheduler(ctx: typing.Any) -> None:
    context: Context = ctx.obj["context"]

    cloudwatch = CloudWatchClient(context.config)

    scheduler = BackgroundScheduler()
    scheduler.add_job(
        _run_cloudwatch_log,
        args=[cloudwatch],
        id="cloudwatch_log",
        name="cloudwatch_log",
        trigger="cron",
        minute="*",
    )

    scheduler.start()

    runner = init_runner(context, "SCHEDULER")

    runner()

    scheduler.remove_job("cloudwatch_log")


def _run_cloudwatch_log(client: CloudWatchClient) -> None:
    try:
        client.put_metric(
            "TaeinStore",
            client.get_metric_data(
                "TaeinStoreHardwareUsage",
                "store_cpu",
                psutil.cpu_percent(),
            ),
        )
        client.put_metric(
            "TaeinStore",
            client.get_metric_data(
                "TaeinStoreHardwareUsage",
                "store_ram",
                psutil.virtual_memory().percent,
            ),
        )
    except Exception as e:
        logger.error("Exception while cloudwatch scheduler", exc_info=e)


def main() -> None:
    cli()


if __name__ == "__main__":
    main()
