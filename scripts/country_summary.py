import logging
import re

from pandas import read_excel
from requests import get

from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.utilities.dictandlist import write_list_to_csv
from hdx.utilities.downloader import DownloadError

logger = logging.getLogger(__name__)


def country_summary(
        configuration,
        countries,
        temp_folder,
):
    logger.info(f"Summarizing CODs by country")

    results = list()
    headers = [
        "ISO",
        "COD-AB URL",
        "COD-AB level",
        "COD-AB contributor",
        "COD-AB ADM1 units",
        "COD-AB ADM2 units",
        "COD-AB ADM3 units",
        "COD-AB ADM4 units",
        "COD-AB services",
        "COD-PS URL",
        "COD-PS level",
        "COD-PS contributor",
        "COD-PS services",
        "COD-EM URL",
        "COD-EM level",
        "COD-EM services",
    ]

    for iso in countries:
        country_info = dict.fromkeys(headers)
        country_info["ISO"] = iso

        country_datasets = dict()
        country_datasets["COD-AB"] = f"cod-ab-{iso.lower()}"
        country_datasets["COD-PS"] = f"cod-ps-{iso.lower()}"
        country_datasets["COD-EM"] = f"cod-em-{iso.lower()}"

        for cod_type, dataset_name in country_datasets.items():
            try:
                dataset = Dataset.read_from_hdx(dataset_name)
            except HDXError:
                continue
            if not dataset:
                continue

            level = dataset.get("cod_level")
            if not level:
                logger.error(f"Dataset missing level {dataset['name']}")

            country_info[f"{cod_type} level"] = level

            country_info[f"{cod_type} URL"] = dataset.get_hdx_url()
            country_info[f"{cod_type} contributor"] = dataset.get_organization()["title"]

            services = "False"
            if "geoservice" in dataset.get_filetypes():
                services = "True"
            if cod_type == "COD-PS":
                itos_presence = get(configuration["itos_ps_url"] + iso).status_code
                if itos_presence == 200:
                    services = "True"
            country_info[f"{cod_type} services"] = services

            if cod_type == "COD-AB":
                resources = [r for r in dataset.get_resources() if r.get_format() in ["xls", "xlsx"]]
                if len(resources) > 1:
                    resources = [
                        r for r in resources if bool(re.match(
                            "(.*adm(in)?.?boundaries.?tabular.?data.*)|(.*adm_?ga?z.*)|(.*gazetteer.*)|(.*adm.*)",
                            r["name"], re.IGNORECASE
                        ))
                    ]
                if len(resources) == 0:
                    logger.warning(f"Cannot find gazetteer for {iso}")
                    continue

                try:
                    _, resource_file = resources[0].download(folder=temp_folder)
                except DownloadError:
                    logger.warning(f"Could not download gazetteer for {iso}")

                contents = read_excel(resource_file, sheet_name=None)
                for sheet_name, sheet in contents.items():
                    level = re.search("adm(in)?.?[1-7]", sheet_name, re.IGNORECASE)
                    if not level:
                        continue
                    sheet.dropna(axis=0, how="all", inplace=True)
                    adm_level = level.group()[-1]
                    rows = len(sheet)
                    country_info[f"{cod_type} ADM{adm_level} units"] = rows

        if len([c for c in country_info.values() if c]) > 1:
            results.append(country_info)

    write_list_to_csv("country_summary.csv", results, headers=headers)

    logger.info("Wrote out country summary")
    return
