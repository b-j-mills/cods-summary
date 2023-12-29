import logging

from hdx.data.dataset import Dataset
from hdx.utilities.dictandlist import write_list_to_csv

logger = logging.getLogger(__name__)


def dataset_resource_descriptions():

    datasets = Dataset.search_in_hdx(
        fq='cod_level:"cod-standard"'
    ) + Dataset.search_in_hdx(
        fq='cod_level:"cod-enhanced"'
    )

    logger.info(f"Summarizing metadata for {len(datasets)} COD datasets")

    results = [[
                "country",
                "theme",
                "level",
                "dataset name",
                "item",
                "title",
                "description",
            ]]

    for dataset in datasets:
        theme = None
        if dataset["name"][:6] in ["cod-ab", "cod-ps", "cod-hp", "cod-em"]:
            theme = dataset["name"][4:6].upper()

        if not theme:
            continue

        country = " | ".join(dataset.get_location_iso3s())

        results.append(
            [
                country,
                theme,
                dataset["cod_level"],
                dataset["name"],
                "dataset",
                dataset["title"],
                dataset["notes"],
            ]
        )

        for resource in dataset.get_resources():
            results.append(
                [
                    country,
                    theme,
                    dataset.get("cod_level"),
                    dataset["name"],
                    "resource",
                    resource["name"],
                    resource["description"],
                ]
            )

    write_list_to_csv("dataset_resource_descriptions.csv", results)

    logger.info("Wrote out descriptions")
    return
