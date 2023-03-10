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

    itos_datasets = None
    itos_names = []
    try:
        itos_datasets = get(configuration["itos_url"]).json()
    except ConnectTimeout:
        logger.error("Could not connect to ITOS API")
    if itos_datasets:
        for i in itos_datasets:
            location = i["Location"]
            theme = i["Theme"]
            if theme == "COD_AB" and (location == ["MMR"] or location == ["mmr"]):
                name = slugify(i["DatasetTitle"])
            else:
                name = slugify(f"{theme} {' '.join(location)}")
            itos_names.append(name)

    results = [[
                "COD-UID",
                "dataset title",
                "URL",
                "Theme",
                "Level",
                "in ITOS API",
                "number of resource downloads",
                "source",
                "contributor/organization",
                "date of dataset",
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
        theme = None
        if dataset["name"][:6] in ["cod-ab", "cod-ps", "cod-hp", "cod-em"]:
            theme = dataset["name"][4:6].upper()

        in_itos = "No"
        if dataset["name"] in itos_names:
            in_itos = "Yes"

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
                in_itos,
                dataset.get("total_res_downloads"),
                dataset.get("dataset_source"),
                dataset.get_organization()["title"],
                dataset.get("dataset_date"),
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
