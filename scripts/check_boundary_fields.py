import csv
import logging
import re

from geopandas import read_file
from glob import glob
from os.path import join
from zipfile import BadZipFile, ZipFile

from hdx.data.dataset import Dataset
from hdx.utilities.downloader import DownloadError
from hdx.utilities.uuid import get_uuid

logger = logging.getLogger(__name__)


def check_boundary_fields(
    configuration,
    countries,
    temp_folder,
):
    if not countries or countries == "all":
        countries = configuration["countries"]

    dataset_exceptions = configuration["boundaries"].get("dataset_exceptions")
    if not dataset_exceptions:
        dataset_exceptions = {}

    with open("boundary_dataset_headers.csv", "w") as c:
        writer = csv.writer(c)
        writer.writerow(
            [
                "ISO",
                "COD-UID",
                "contributor",
                "error",
                "resource name",
                "file name",
                "pcode headers",
                "name headers",
            ]
        )

        for iso in countries:
            logger.info(f"Processing boundaries for {iso}")

            row = [iso, None, None, None, None, None, None, None]

            dataset_names = dataset_exceptions.get(iso)
            if not dataset_names:
                dataset_names = [f"cod-ab-{iso.lower()}"]

            for dataset_name in dataset_names:
                dataset = Dataset.read_from_hdx(dataset_name)
                if not dataset:
                    logger.warning(f"Could not find dataset {dataset_name}")
                    continue

                row[1] = dataset.get_hdx_url().split("/")[-1]
                row[2] = dataset.get_organization()["title"]

                resources = dataset.get_resources()
                resource_list = []
                for r in resources:
                    if r.get_file_type().lower() in ["shp", "geojson"]:
                        resource_list.append(r)

                if len(resource_list) == 0:
                    row[3] = "Could not find shp or json boundary resource"
                    writer.writerow(row)
                    logger.error(f"Could not find resources from {dataset_name}")
                    continue

                for resource in resource_list:
                    row = row[:3] + [None] * 5
                    row[4] = resource["name"]
                    try:
                        _, resource_file = resource.download(folder=temp_folder)
                    except DownloadError:
                        row[3] = "Could not download boundary resource"
                        writer.writerow(row)
                        logger.error(f"Could not download resource")
                        continue

                    if resource.get_file_type() == "shp":
                        temp_dir = join(temp_folder, get_uuid())
                        try:
                            with ZipFile(resource_file, "r") as z:
                                z.extractall(temp_dir)
                        except BadZipFile:
                            row[3] = "Could not unzip boundary resource"
                            writer.writerow(row)
                            logger.error("Could not unzip file!")
                            continue
                        out_files = glob(join(temp_dir, "**", "*.shp"), recursive=True)
                    else:
                        out_files = [resource_file]

                    if len(out_files) == 0:
                        row[3] = "Could not find shp in zip"
                        writer.writerow(row)
                        logger.error("Could not find shp in zip!")
                        continue

                    for out_file in out_files:
                        row = row[:5] + [None] * 3

                        try:
                            boundary_lyr = read_file(out_file)
                        except:
                            logger.error(f"Could not open {out_file}")
                            row[3] = "Could not read file"
                            writer.writerow(row)
                            continue

                        fields = boundary_lyr.columns
                        pcode_fields = []
                        name_fields = []
                        for field in fields:
                            codematch = bool(re.search("p?code", field, re.IGNORECASE))
                            namematch = bool(re.search("name|_(en|fr|es|ru)$", field, re.IGNORECASE))
                            levelmatch = bool(
                                re.search("(^\d\D)|(\D\d\D)|(\D\d$)", field, re.IGNORECASE)
                            )
                            if codematch and levelmatch:
                                pcode_fields.append(field)
                            if namematch and levelmatch:
                                name_fields.append(field)

                        if len(pcode_fields) > 0:
                            row[6] = ", ".join(list(set(pcode_fields)))

                        if len(name_fields) > 0:
                            row[7] = ", ".join(list(set(name_fields)))

                        writer.writerow(row)

    logger.info("Wrote out boundary fields")
    return
