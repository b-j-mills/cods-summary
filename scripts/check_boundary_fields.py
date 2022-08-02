import csv
import logging
import re
#from geopandas import read_file
from glob import glob
from os.path import join
from zipfile import ZipFile, BadZipFile

from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.utilities.downloader import DownloadError
from hdx.utilities.uuid import get_uuid

logger = logging.getLogger(__name__)


def check_boundary_fields(
        configuration,
        downloader,
        temp_folder,
):

    countries = configuration["countries"]

    dataset_exceptions = configuration["boundaries"].get("dataset_exceptions")
    if not dataset_exceptions:
        dataset_exceptions = {}

    with open("boundary_dataset_adm1_headers.csv", "w") as c:
        writer = csv.writer(c)
        writer.writerow(
            ["ISO", "COD-UID", "resource name", "pcode header", "name header"])

        for iso in countries:
            logger.info(f"Processing boundaries for {iso}")

            row = [iso, None, None, None, None]

            dataset_name = dataset_exceptions.get(iso)
            if not dataset_name:
                dataset_name = f"cod-ab-{iso.lower()}"

            try:
                dataset = Dataset.read_from_hdx(dataset_name)
            except HDXError:
                logger.warning(f"Could not find dataset {dataset_name}")
                writer.writerow(row)
                continue
            if not dataset:
                writer.writerow(row)
                continue

            row[1] = dataset.get_hdx_url().split("/")[-1]

            resources = dataset.get_resources()
            resource_list = []
            for r in resources:
                if r.get_file_type().lower() == "shp":
                    resource_list.append(r)

            if len(resource_list) == 0:
                row[2] = "Could not find admin1 boundary resource"
                writer.writerow(row)
                logger.error(f"Could not find resource from {dataset_name}")
                continue

            row[2] = resource_list[0]["name"]

            try:
                _, resource_file = resource_list[0].download(folder=temp_folder)
            except DownloadError:
                row[2] = "Could not download admin1 boundary resource"
                writer.writerow(row)
                logger.error(f"Could not download resource")
                continue

            temp_dir = join(temp_folder, get_uuid())
            try:
                with ZipFile(resource_file, "r") as z:
                    z.extractall(temp_dir)
            except BadZipFile:
                row[2] = "Could not unzip boundary resource"
                writer.writerow(row)
                logger.error("Could not unzip file!")
                continue
            out_files = glob(join(temp_dir, "**", "*.shp"), recursive=True)

            if len(out_files) == 0:
                row[2] = "Could not find shp in zip"
                writer.writerow(row)
                logger.error("Could not find shp in zip!")
                continue

            if len(out_files) > 1:
                name_match = [
                    bool(re.match(".*admbnda.*adm(in)?(0)?1.*", b, re.IGNORECASE))
                    for b in out_files
                ]
                if any(name_match):
                    out_files = [
                        out_files[i] for i in range(len(out_files)) if name_match[i]
                    ]
            if len(out_files) > 1:
                simp_match = [
                    bool(re.match(".*simplified.*", b, re.IGNORECASE)) for b in out_files
                ]
                if any(simp_match):
                    out_files = [
                        out_files[i]
                        for i in range(len(out_files))
                        if not simp_match[i]
                    ]

            if len(out_files) > 1:
                row[2] = "Can't distinguish adm1 shp in zip"
                writer.writerow(row)
                logger.error("Can't distinguish adm1 shp in zip")
                continue

            # boundary_lyr = read_file(out_files[0])
            #
            # fields = boundary_lyr.columns
            # pcode_field = None
            # name_field = None
            #
            # if "ADM1_PCODE" in fields:
            #     pcode_field = "ADM1_PCODE"
            # if "ADM1_EN" in fields:
            #     name_field = "ADM1_EN"
            # for field in fields:
            #
            #     if not pcode_field:
            #         if (
            #                 field.upper()
            #                 in configuration["shapefile_attribute_mappings"]["pcode"]
            #         ):
            #             pcode_field = field
            #     if not name_field:
            #         if (
            #                 field.upper()
            #                 in configuration["shapefile_attribute_mappings"]["name"]
            #         ):
            #             name_field = field
            # for header in headers:
            #     if not pcode_header:
            #         codematch = bool(re.match(".*p?code.*", header, re.IGNORECASE))
            #         levelmatch = bool(re.match(".*1.*", header, re.IGNORECASE))
            #         if codematch and levelmatch:
            #             pcode_header = header
            #     popmatch = bool(
            #         re.search(
            #             "(population|both|total|proj|pop|ensemble)",
            #             header,
            #             re.IGNORECASE,
            #         )
            #     )
            #     tmatch = False
            #     if header.lower() in ["t", "t_tl"]:
            #         tmatch = True
            #     sexyearmatch = bool(
            #         re.search("_f|_m|m_|f_|year|female|male|trans", header, re.IGNORECASE)
            #     )
            #     agematch = bool(
            #         re.search("^\d{1,2}\D|(\D\d{1,2}\D)|(\D\d$)", header, re.IGNORECASE)
            #     )
            #     agewordmatch = bool(re.search("(age|adult|plus)", header, re.IGNORECASE))
            #     urmatch = bool(re.search("(urban|rural)", header, re.IGNORECASE))
            #     if (
            #         (popmatch or tmatch)
            #         and not sexyearmatch
            #         and not agematch
            #         and not agewordmatch
            #         and not urmatch
            #     ):
            #         pop_header.append(header)
            #
            # if pcode_header:
            #     row[3] = pcode_header
            #
            # if len(pop_header) == 1:
            #     row[4] = pop_header[0]
            #
            # if len(pop_header) > 1:
            #     yearmatches = [re.findall("(?<!\d)\d{4}(?!\d)", header, re.IGNORECASE) for header in pop_header]
            #     yearmatches = sum(yearmatches, [])
            #     if len(yearmatches) == 0:
            #         logger.info(f"Not sure which header to pick: {pop_header}")
            #         row[4] = ",".join(pop_header)
            #         writer.writerow(row)
            #         continue
            #     yearmatches = [int(y) for y in yearmatches]
            #     maxyear = [h for h in pop_header if str(max(yearmatches)) in h]
            #     if len(maxyear) != 1:
            #         logger.info(f"Not sure which header to pick: {pop_header}")
            #         row[4] = ",".join(pop_header)
            #         writer.writerow(row)
            #         continue
            #     row[4] = maxyear[0]
            #
            # writer.writerow(row)

    logger.info("Wrote out population headers")
    return
