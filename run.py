import logging
from os.path import join, expanduser

from hdx.api.configuration import Configuration
from hdx.facades.simple import facade
from hdx.utilities.downloader import Download
from scripts.check_population_headers import check_population_headers
from scripts.metadata_summary import metadata_summary

logger = logging.getLogger(__name__)

lookup = "cods-summary"


def main():
    configuration = Configuration.read()

    with Download(rate_limit={"calls": 1, "period": 0.1}) as downloader:
        metadata_summary(configuration)
        check_population_headers(configuration, downloader)


if __name__ == "__main__":
    facade(
        main,
        hdx_site="prod",
        hdx_read_only=True,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yml"),
        user_agent_lookup=lookup,
        project_config_yaml=join("config", "project_configuration.yml"),
    )
