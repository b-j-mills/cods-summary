import logging

from hdx.data.dataset import Dataset

logger = logging.getLogger(__name__)


def cowboy_cods(errors_on_exit):
    logger.info("Finding cowboy CODs")

    datasets = Dataset.search_in_hdx(fq='vocab_Topics:"common operational dataset-cod"')

    if len(datasets) == 0:
        return

    dataset_names = ",".join([d.get("name") for d in datasets])
    errors_on_exit.add(f"Found {len(datasets)} dataset(s) with COD tags: {dataset_names}")

    datasets_without_level = [d for d in datasets if not d.get("cod_level")]

    if len(datasets_without_level) == 0:
        return

    dataset_names = ",".join([d.get("name") for d in datasets_without_level])
    errors_on_exit.add(f"Found {len(datasets_without_level)} cowboy COD(s): {dataset_names}")

    return
