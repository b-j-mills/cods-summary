import logging
import re

from pandas import read_csv

from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.utilities.dictandlist import write_list_to_csv
from hdx.utilities.downloader import DownloadError

logger = logging.getLogger(__name__)


def country_ps_summary(
        countries,
        temp_folder,
):
    logger.info(f"Summarizing COD PS by country")

    results = list()
    headers = [
        "ISO",
        "COD-PS URL",
        "COD-PS level",
        "COD-PS contributor",
        "COD-PS description",
        "COD-PS ADM1 units",
        "COD-PS ADM2 units",
        "COD-PS ADM3 units",
        "COD-PS ADM4 units",
    ]

    for iso in countries:
        country_info = dict.fromkeys(headers)
        country_info["ISO"] = iso

        dataset = None
        try:
            dataset = Dataset.read_from_hdx(f"cod-ps-{iso.lower()}")
        except HDXError:
            continue
        if not dataset:
            continue

        country_info["COD-PS URL"] = dataset.get_hdx_url()
        country_info["COD-PS contributor"] = dataset.get_organization()["title"]
        country_info["COD-PS description"] = dataset["notes"]

        level = dataset.get("cod_level")
        if not level:
            logger.error(f"Dataset missing level {dataset['name']}")
        country_info["COD-PS level"] = level

        resources = [r for r in dataset.get_resources() if r.get_format() == "csv"]
        if len(resources) == 0:
            logger.warning(f"No csv resources found for {dataset['name']}")

        missing_levels = []
        for adm_level in ["1", "2", "3", "4"]:
            adm_resources = [
                r for r in resources if bool(re.match(f".*adm(in)?_?{adm_level}.*", r["name"], re.IGNORECASE))
            ]
            if len(adm_resources) == 0:
                missing_levels.append(adm_level)
                continue

            if len(adm_resources) > 1:
                logger.warning(f"Multiple adm{adm_level} resources for {dataset['name']}")

            try:
                _, resource_file = adm_resources[0].download(folder=temp_folder)
            except DownloadError:
                logger.error(f"Could not download adm{adm_level} pop spreadsheet for {iso}")
                continue
            try:
                contents = read_csv(resource_file)
            except:
                logger.error(f"Could not open adm{adm_level} pop spreadsheet for {iso}")
                continue

            contents.dropna(axis=0, how="all", inplace=True)
            rows = len(contents)
            cell0 = contents.iloc[0, 0]
            if str(cell0)[0] == "#":
                rows = len(contents) - 1
            country_info[f"COD-PS ADM{adm_level} units"] = rows

        expected_missing_levels = [str(i) for i in range(5-len(missing_levels), 5)]
        if missing_levels != expected_missing_levels:
            logger.error(f"{iso} missing unexpected levels: {missing_levels}")

        if len([c for c in country_info.values() if c]) > 1:
            results.append(country_info)

    write_list_to_csv("country_ps_summary.csv", results, headers=headers)

    logger.info("Wrote out country PS summary")
    return
