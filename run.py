import csv
import logging
import requests
from os.path import join, expanduser

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.facades.keyword_arguments import facade

logger = logging.getLogger(__name__)

lookup = "cods-summary"


def main():
    configuration = Configuration.read()
    datasets = Dataset.search_in_hdx(
        fq='vocab_Topics:"common operational dataset - cod"'
    )
    logger.info(f"Found {len(datasets)} datasets")

    itos_datasets = requests.get(configuration["itos_url"]).json()
    itos_titles = [d["DatasetTitle"] for d in itos_datasets]

    with open("datasets_tagged_cods.csv", "w") as c:
        writer = csv.writer(c)
        writer.writerow(["dataset title", "URL", "in ITOS API", "source", "contributor/organization",
                         "date of dataset", "updated", "expected update frequency", "location",
                         "visibility", "license", "methodology", "caveats", "tags", "file formats"])

        for dataset in datasets:
            tags = dataset.get_tags()
            if "common operational dataset - cod" not in tags:
                continue
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
                    dataset["title"],
                    dataset.get_hdx_url(),
                    in_itos,
                    dataset["dataset_source"],
                    dataset.get_organization()['title'],
                    dataset["dataset_date"],
                    dataset["last_modified"],
                    dataset.transform_update_frequency(dataset["data_update_frequency"]),
                    " | ".join(dataset.get_location_iso3s()),
                    visibility,
                    dataset["license_title"],
                    methodology,
                    dataset.get("caveats"),
                    " | ".join(tags),
                    " | ".join(formats),
                ]
            )

    logger.info("Wrote out information")


if __name__ == "__main__":
    facade(
        main,
        hdx_site="prod",
        hdx_read_only=True,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yml"),
        user_agent_lookup=lookup,
        project_config_yaml=join("config", "project_configuration.yml"),
    )
