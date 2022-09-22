import csv
import logging
from requests import get
from requests.exceptions import ConnectTimeout

from hdx.data.dataset import Dataset

logger = logging.getLogger(__name__)

lookup = "cods-summary"


def metadata_summary(
    configuration,
):

    datasets = Dataset.search_in_hdx(
        fq='vocab_Topics:"common operational dataset - cod"'
    )
    logger.info(f"Summarizing metadata for {len(datasets)} COD datasets")

    itos_datasets = None
    itos_titles = []
    try:
        itos_datasets = get(configuration["itos_url"]).json()
    except ConnectTimeout:
        logger.error("Could not connect to ITOS API")
    if itos_datasets:
        itos_titles = [d["DatasetTitle"] for d in itos_datasets]

    with open("../datasets_tagged_cods.csv", "w") as c:
        writer = csv.writer(c)
        writer.writerow(
            [
                "COD-UID",
                "dataset title",
                "URL",
                "Theme",
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
            ]
        )

        for dataset in datasets:
            tags = dataset.get_tags()
            if "common operational dataset - cod" not in tags:
                continue

            theme = None
            if dataset["name"][:6] in ["cod-ab", "cod-ps", "cod-hp", "cod-em"]:
                theme = dataset["name"][4:6].upper()

            in_itos = "No"
            if dataset["title"] in itos_titles:
                in_itos = "Yes"

            methodology = dataset["methodology"]
            if methodology == "Other":
                methodology = dataset["methodology_other"]

            visibility = "Visible"
            if dataset["is_requestdata_type"]:
                visibility = "Available by request"

            formats = []
            resources = dataset.get_resources()
            for r in resources:
                formats.append(r.get_file_type())
            formats = list(set(formats))

            writer.writerow(
                [
                    dataset.get_hdx_url().split("/")[-1],
                    dataset["title"],
                    dataset.get_hdx_url(),
                    theme,
                    in_itos,
                    dataset["total_res_downloads"],
                    dataset["dataset_source"],
                    dataset.get_organization()["title"],
                    dataset["dataset_date"],
                    dataset["last_modified"],
                    dataset.transform_update_frequency(
                        dataset["data_update_frequency"]
                    ),
                    " | ".join(dataset.get_location_iso3s()),
                    visibility,
                    dataset["license_title"],
                    methodology,
                    dataset.get("caveats"),
                    " | ".join(tags),
                    " | ".join(formats),
                ]
            )

    logger.info("Wrote out metadata")
    return
