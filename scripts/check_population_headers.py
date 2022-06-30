import csv
import logging
import re

from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError

logger = logging.getLogger(__name__)


def check_population_headers(
        configuration,
        downloader,
):

    countries = configuration["countries"]

    resource_exceptions = configuration["population"].get("resource_exceptions")
    if not resource_exceptions:
        resource_exceptions = {}

    with open("population_dataset_adm1_headers.csv", "w") as c:
        writer = csv.writer(c)
        writer.writerow(
            ["ISO", "COD-UID", "resource name", "pcode header", "total pop header"])

        for iso in countries:
            logger.info(f"Processing population for {iso}")

            row = [iso, None, None, None, None]

            dataset_name = f"cod-ps-{iso.lower()}"
            resource_name = resource_exceptions.get(iso)
            if not resource_name:
                resource_name = "adm(in)?1"

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
                if r.get_file_type().lower() == "csv":
                    if bool(re.match(f".*{resource_name}.*", r["name"], re.IGNORECASE)):
                        resource_list.append(r)

            if len(resource_list) == 0:
                row[2] = "Could not find admin1 csv resource"
                writer.writerow(row)
                logger.error(f"Could not find resource from {dataset_name}")
                continue

            if len(resource_list) > 1:
                yearmatches = [re.findall("(?<!\d)\d{4}(?!\d)", r["name"], re.IGNORECASE) for r in resource_list]
                yearmatches = sum(yearmatches, [])
                if len(yearmatches) > 0:
                    yearmatches = [int(y) for y in yearmatches]
                maxyear = [r for r in resource_list if str(max(yearmatches)) in r["name"]]
                if len(maxyear) == 1:
                    resource_list = maxyear

            if len(resource_list) > 1:
                logger.warning(f"Found multiple resources for {iso}, using first in list")

            row[2] = resource_list[0]["name"]

            headers, _ = downloader.get_tabular_rows(
                resource_list[0]["url"], dict_form=True
            )

            pcode_header = None
            pop_header = []
            for header in headers:
                if not pcode_header:
                    codematch = bool(re.match(".*p?code.*", header, re.IGNORECASE))
                    levelmatch = bool(re.match(".*1.*", header, re.IGNORECASE))
                    if codematch and levelmatch:
                        pcode_header = header
                popmatch = bool(
                    re.search(
                        "(population|both|total|proj|pop|ensemble)",
                        header,
                        re.IGNORECASE,
                    )
                )
                tmatch = False
                if header.lower() in ["t", "t_tl"]:
                    tmatch = True
                sexyearmatch = bool(
                    re.search("_f|_m|m_|f_|year|female|male|trans", header, re.IGNORECASE)
                )
                agematch = bool(
                    re.search("^\d{1,2}\D|(\D\d{1,2}\D)|(\D\d$)", header, re.IGNORECASE)
                )
                agewordmatch = bool(re.search("(age|adult|plus)", header, re.IGNORECASE))
                urmatch = bool(re.search("(urban|rural)", header, re.IGNORECASE))
                if (
                    (popmatch or tmatch)
                    and not sexyearmatch
                    and not agematch
                    and not agewordmatch
                    and not urmatch
                ):
                    pop_header.append(header)

            if pcode_header:
                row[3] = pcode_header

            if len(pop_header) == 1:
                row[4] = pop_header[0]

            if len(pop_header) > 1:
                yearmatches = [re.findall("(?<!\d)\d{4}(?!\d)", header, re.IGNORECASE) for header in pop_header]
                yearmatches = sum(yearmatches, [])
                if len(yearmatches) == 0:
                    logger.info(f"Not sure which header to pick: {pop_header}")
                    row[4] = ",".join(pop_header)
                    writer.writerow(row)
                    continue
                yearmatches = [int(y) for y in yearmatches]
                maxyear = [h for h in pop_header if str(max(yearmatches)) in h]
                if len(maxyear) != 1:
                    logger.info(f"Not sure which header to pick: {pop_header}")
                    row[4] = ",".join(pop_header)
                    writer.writerow(row)
                    continue
                row[4] = maxyear[0]

            writer.writerow(row)

    logger.info("Wrote out population headers")
    return
