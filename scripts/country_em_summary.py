import logging
import re

from pandas import read_excel
from requests import get

from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.utilities.dictandlist import write_list_to_csv
from hdx.utilities.downloader import DownloadError

logger = logging.getLogger(__name__)


def country_em_summary(
        countries,
        temp_folder,
):
    logger.info(f"Summarizing COD EM by country")

    results = list()
    headers = [
        "ISO",
        "COD-EM URL",
        "COD-EM level",
        "COD-EM contributor",
        "COD-EM description",
        "COD-EM ADM1 units",
        "COD-EM ADM2 units",
        "COD-EM ADM3 units",
        "COD-EM ADM4 units",
    ]

    for iso in countries:
        country_info = dict.fromkeys(headers)
        country_info["ISO"] = iso

        dataset = None
        try:
            dataset = Dataset.read_from_hdx(f"cod-em-{iso.lower()}")
        except HDXError:
            continue
        if not dataset:
            continue
        if dataset["archived"]:
            continue

        country_info["COD-EM URL"] = dataset.get_hdx_url()
        country_info["COD-EM contributor"] = dataset.get_organization()["title"]
        country_info["COD-EM description"] = dataset["notes"]

        level = dataset.get("cod_level")
        if not level:
            logger.error(f"Dataset missing level {dataset['name']}")
        country_info["COD-EM level"] = level

        resources = [r for r in dataset.get_resources() if r.get_format() in ["xls", "xlsx"]]
        if len(resources) > 1:
            resources = [
                r for r in resources if bool(re.match(
                    "(.*adm(in)?.?boundaries.?tabular.?data.*)|(.*adm_?ga?z.*)|(.*gazetteer.*)|(.*adm.*)",
                    r["name"], re.IGNORECASE
                ))
            ]
        if len(resources) == 0:
            logger.warning(f"Cannot find gazetteer for COD-EM {iso}")
            continue

        try:
            _, resource_file = resources[0].download(folder=temp_folder)
        except DownloadError:
            logger.error(f"Could not download gazetteer for COD-EM {iso}")
            continue

        contents = read_excel(resource_file, sheet_name=None)
        for sheet_name, sheet in contents.items():
            level = re.search("adm(in)?.?[1-7]", sheet_name, re.IGNORECASE)
            if not level:
                continue
            sheet.dropna(axis=0, how="all", inplace=True)
            adm_level = level.group()[-1]
            rows = len(sheet)
            country_info[f"COD-EM ADM{adm_level} units"] = rows

        if len([c for c in country_info.values() if c]) > 1:
            results.append(country_info)

    write_list_to_csv("country_em_summary.csv", results, headers=headers)

    logger.info("Wrote out country EM summary")
    return
