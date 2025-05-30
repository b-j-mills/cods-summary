import argparse
import logging
import warnings
from os import getenv
from os.path import expanduser, join

from hdx.api.configuration import Configuration
from hdx.facades.keyword_arguments import facade
from hdx.location.country import Country
from hdx.utilities.downloader import Download
from hdx.utilities.errors_onexit import ErrorsOnExit
from hdx.utilities.path import temp_dir
from scripts.check_boundary_fields import check_boundary_fields
from scripts.check_population_headers import check_population_headers
from scripts.country_ab_summary import country_ab_summary
from scripts.country_em_summary import country_em_summary
from scripts.country_ps_summary import country_ps_summary
from scripts.cowboy_cods import cowboy_cods
from scripts.dataset_resource_descriptions import dataset_resource_descriptions
from scripts.metadata_summary import metadata_summary

warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

logger = logging.getLogger(__name__)

lookup = "cods-summary"


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-hs", "--hdxsite", default=None, help="HDX site")
    parser.add_argument("-sc", "--scrapers", default=None, help="Scrapers to run")
    parser.add_argument("-co", "--countries", default=None, help="Which countries to check")
    args = parser.parse_args()
    return args


def main(
    scrapers_to_run,
    countries,
    **ignore,
):
    if not countries or countries == "all":
        countries = [key for key in Country.countriesdata()["countries"]]

    configuration = Configuration.read()
    with ErrorsOnExit() as errors_on_exit:
        with temp_dir() as temp_folder:
            with Download(rate_limit={"calls": 1, "period": 0.1}) as downloader:
                open("errors.txt", "w").close()

                if scrapers_to_run:
                    logger.info(f"Running only scrapers: {scrapers_to_run}")
                if "metadata_summary" in scrapers_to_run:
                    metadata_summary(configuration)
                if "check_population_headers" in scrapers_to_run:
                    check_population_headers(downloader, countries)
                if "check_boundary_fields" in scrapers_to_run:
                    check_boundary_fields(configuration, countries, temp_folder)
                if "cowboy_cods" in scrapers_to_run:
                    cowboy_cods(errors_on_exit)
                if "country_ab_summary" in scrapers_to_run:
                    country_ab_summary(countries, temp_folder)
                if "country_em_summary" in scrapers_to_run:
                    country_em_summary(countries, temp_folder)
                if "country_ps_summary" in scrapers_to_run:
                    country_ps_summary(countries, temp_folder)
                if "dataset_resource_descriptions" in scrapers_to_run:
                    dataset_resource_descriptions()

            if len(errors_on_exit.errors) > 0:
                with open("errors.txt", "w") as fp:
                    fp.write("\n".join(errors_on_exit.errors))


if __name__ == "__main__":
    args = parse_args()
    hdx_site = args.hdxsite
    if not hdx_site:
        hdx_site = getenv("HDX_SITE", "prod")
    scrapers_to_run = args.scrapers
    if scrapers_to_run is None:
        scrapers_to_run = getenv(
            "SCRAPERS_TO_RUN",
            "metadata_summary,country_ab_summary,country_em_summary,country_ps_summary,cowboy_cods",
        )
    if scrapers_to_run:
        scrapers_to_run = scrapers_to_run.split(",")
    countries = args.countries
    if countries:
        countries = countries.split(",")
    if countries is None:
        countries = "all"
    facade(
        main,
        scrapers_to_run=scrapers_to_run,
        countries=countries,
        hdx_site=hdx_site,
        hdx_read_only=True,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=lookup,
    )
