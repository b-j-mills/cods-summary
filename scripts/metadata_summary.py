import logging
from requests import get
from requests.exceptions import ConnectTimeout
from slugify import slugify

from hdx.data.dataset import Dataset
from hdx.utilities.dictandlist import write_list_to_csv

logger = logging.getLogger(__name__)

lookup = "cods-summary"


def metadata_summary(
    configuration,
):

    datasets = Dataset.search_in_hdx(
        fq='cod_level:"cod-standard"'
    ) + Dataset.search_in_hdx(
        fq='cod_level:"cod-enhanced"'
    ) + Dataset.search_in_hdx(fq='vocab_Topics:"common operational dataset-cod"')

    logger.info(f"Summarizing metadata for {len(datasets)} COD datasets")

    results = [[
                "COD-UID",
                "dataset title",
                "URL",
                "Theme",
                "Level",
                "number of resource downloads",
                "source",
                "contributor/organization",
                "start date of dataset",
                "end date of dataset",
                "updated",
                "expected update frequency",
                "location",
                "visibility",
                "license",
                "methodology",
                "caveats",
                "tags",
                "file formats",
            ]]

    for dataset in datasets:
        if dataset["name"][:3] != "cod":
            continue

        theme = None
        if dataset["name"][:6] in ["cod-ab", "cod-ps", "cod-hp", "cod-em"]:
            theme = dataset["name"][4:6].upper()

        methodology = dataset.get("methodology")
        if methodology == "Other":
            methodology = dataset.get("methodology_other")

        visibility = "Visible"
        if dataset.get("is_requestdata_type"):
            visibility = "Available by request"

        results.append(
            [
                dataset.get_hdx_url().split("/")[-1],
                dataset.get("title"),
                dataset.get_hdx_url(),
                theme,
                dataset.get("cod_level"),
                dataset.get("total_res_downloads"),
                dataset.get("dataset_source"),
                dataset.get_organization()["title"],
                dataset.get_time_period("%d-%m-%Y")["startdate_str"],
                dataset.get_time_period("%d-%m-%Y")["enddate_str"],
                dataset.get("last_modified"),
                dataset.transform_update_frequency(
                    dataset.get("data_update_frequency")
                ),
                " | ".join(dataset.get_location_iso3s()),
                visibility,
                dataset.get("license_title"),
                methodology,
                dataset.get("caveats"),
                " | ".join(dataset.get_tags()),
                " | ".join(dataset.get_filetypes()),
            ]
        )

    write_list_to_csv("datasets_tagged_cods.csv", results)

    logger.info("Wrote out metadata")
    return
