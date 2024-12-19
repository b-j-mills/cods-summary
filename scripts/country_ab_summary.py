import logging
import re

from pandas import read_excel

from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.utilities.dictandlist import write_list_to_csv
from hdx.utilities.downloader import DownloadError

logger = logging.getLogger(__name__)


def country_ab_summary(
        countries,
        temp_folder,
):
    logger.info(f"Summarizing COD AB by country")

    results = list()
    headers = [
        "ISO",
        "COD-AB URL",
        "COD-AB level",
        "COD-AB contributor",
        "COD-AB description",
        "COD-AB ADM1 units",
        "COD-AB ADM2 units",
        "COD-AB ADM3 units",
        "COD-AB ADM4 units",
    ]

    for iso in countries:
        country_info = dict.fromkeys(headers)
        country_info["ISO"] = iso

        dataset = None
        try:
            dataset = Dataset.read_from_hdx(f"cod-ab-{iso.lower()}")
        except HDXError:
            continue
        if not dataset:
            continue
        if dataset["archived"]:
            continue

        country_info["COD-AB URL"] = dataset.get_hdx_url()
        country_info["COD-AB contributor"] = dataset.get_organization()["title"]
        country_info["COD-AB description"] = dataset["notes"]

        level = dataset.get("cod_level")
        if not level:
            logger.error(f"Dataset missing level {dataset['name']}")
        country_info["COD-AB level"] = level

        resources = [r for r in dataset.get_resources() if r.get_format() in ["xls", "xlsx"]]
        if len(resources) > 1:
            resources = [r for r in resources if "gazetteer" in r["description"].lower() or
                         "taxonomy" in r["description"].lower() or
                         bool(re.match(".*adm.*tabular.?data.*", r["name"], re.IGNORECASE))]
        if len(resources) == 0:
            logger.warning(f"Cannot find gazetteer for COD-AB {iso}")
            if len([c for c in country_info.values() if c]) > 1:
                results.append(country_info)
            continue

        if len(resources) > 1:
            logger.warning(f"Found more than one gazetteer for COD-AB {iso}")

        for resource in resources:
            try:
                _, resource_file = resource.download(folder=temp_folder)
            except DownloadError:
                logger.error(f"Could not download gazetteer for COD-AB {iso}")
                continue

            contents = read_excel(resource_file, sheet_name=None)
            for sheet_name, sheet in contents.items():
                level = re.search("adm(in)?.?[1-7]", sheet_name, re.IGNORECASE)
                if not level:
                    continue
                sheet.dropna(axis=0, how="all", inplace=True)
                adm_level = level.group()[-1]
                rows = len(sheet)
                country_info[f"COD-AB ADM{adm_level} units"] = rows

        if len([c for c in country_info.values() if c]) > 1:
            results.append(country_info)

    write_list_to_csv("country_ab_summary.csv", results, headers=headers)

    logger.info("Wrote out country AB summary")
    return
