import logging
import re

from pandas import read_excel
from requests import get

from hdx.data.dataset import Dataset
from hdx.utilities.dictandlist import write_list_to_csv
from hdx.utilities.downloader import DownloadError

logger = logging.getLogger(__name__)


def country_summary(
        configuration,
        temp_folder,
):
    logger.info(f"Summarizing CODs by country")

    countries = configuration["countries"]

    dataset_exceptions = configuration["boundaries"].get("dataset_exceptions", {})

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
        country_datasets["COD-AB"] = dataset_exceptions.get(iso, [f"cod-ab-{iso.lower()}"])
        country_datasets["COD-PS"] = [f"cod-ps-{iso.lower()}"]
        country_datasets["COD-EM"] = [f"cod-em-{iso.lower()}"]

        for cod_type, dataset_names in country_datasets.items():
            datasets = [Dataset.read_from_hdx(d) for d in dataset_names if Dataset.read_from_hdx(d) is not None]
            if len(datasets) == 0:
                continue
            urls = [d.get_hdx_url() for d in datasets]
            country_info[f"{cod_type} URL"] = " | ".join(urls)

            levels = [d.get("cod_level") for d in datasets]
            if None in levels:
                logger.error(f"Dataset missing level {cod_type.lower()}-{iso.lower()}")
                levels = [l for l in levels if l]
            country_info[f"{cod_type} level"] = " | ".join(levels)

            contributors = [d.get_organization()["title"] for d in datasets]
            country_info[f"{cod_type} contributor"] = " | ".join(contributors)

            services = ["True" if "Geoservice" in d.get_filetypes() else "False" for d in datasets]
            if cod_type == "COD-PS":
                itos_presence = get(configuration["itos_ps_url"] + iso).status_code
                if itos_presence == 200:
                    services = ["True"]
            country_info[f"{cod_type} services"] = " | ".join(services)

            if cod_type == "COD-AB" and len(datasets) == 1:
                resources = [
                    r for r in datasets[0].get_resources() if r.get_file_type() in ["xls", "xlsx"] and bool(re.match(
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
