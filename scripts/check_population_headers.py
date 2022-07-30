import csv
import logging
import re
from tabulator.exceptions import EncodingError

from hdx.data.dataset import Dataset
from hdx.utilities.downloader import DownloadError

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
            ["ISO", "COD-UID", "error", "resource name", "pcode headers", "name headers",
             "total pop header", "duplicates", "blanks"]
        )

        for iso in countries:
            logger.info(f"Processing population for {iso}")
            do_not_process = resource_exceptions.get(iso)

            row = [iso, None, None, None, None, None, None, None, None]

            dataset_name = f"cod-ps-{iso.lower()}"
            dataset = Dataset.read_from_hdx(dataset_name)
            if not dataset:
                logger.warning(f"Could not find dataset {dataset_name}")
                continue

            row[1] = dataset.get_hdx_url().split("/")[-1]

            resources = dataset.get_resources()
            resource_list = []
            for resource in resources:
                if resource.get_file_type().lower() != "csv":
                    continue
                if resource["name"] == do_not_process:
                    row[2] = "Could not read resource"
                    row[3] = resource["name"]
                    writer.writerow(row)
                    continue
                resource_list.append(resource)

            if len(resource_list) == 0:
                logger.warning(f"No csv resources found for {iso}")
                row[2] = "No csv resources found"
                writer.writerow(row)
                continue

            for resource in resource_list:
                row = row[:2] + [None]*7
                row[3] = resource["name"]
                try:
                    headers, iterator = downloader.get_tabular_rows(resource["url"])
                except DownloadError:
                    logger.error(f"Could not read resource {resource['name']}")
                    row[2] = "Could not read resource"
                    writer.writerow(row)
                    continue

                if not headers:
                    row[2] = "Could not read resource"
                    writer.writerow(row)
                    continue

                # Find fields with duplicate headers
                header_counts = dict(zip(headers, [headers.count(i) for i in headers]))
                duplicates = []
                for key in header_counts:
                    if header_counts[key] > 1 and key != '':
                        duplicates.append(key)
                if len(duplicates) > 0:
                    row[7] = ", ".join(duplicates)

                # Find fields that are not filled in
                filled = [0]*len(headers)
                try:
                    for r in iterator:
                        filled = [filled[i]+1 if r[i] else filled[i] for i in range(len(r))]
                except EncodingError:
                    logger.error(f"Could not read resource {resource['name']}")
                    row[2] = "Could not read resource"
                    writer.writerow(row)
                    continue

                empties = [headers[i] for i, j in enumerate(filled) if j == 0 and headers[i] != '']
                if len(empties) > 0:
                    row[8] = ", ".join(empties)

                # Find p-code and population headers
                pcode_header = []
                name_header = []
                pop_header = []
                for header in headers:
                    codematch = bool(re.search("p?code", header, re.IGNORECASE))
                    namematch = bool(re.search("name|_en$", header, re.IGNORECASE))
                    levelmatch = bool(re.search("(^\d\D)|(\D\d\D)|(\D\d$)", header, re.IGNORECASE))
                    if codematch and levelmatch:
                        pcode_header.append(header)
                    if namematch and levelmatch:
                        name_header.append(header)
                    if len(pop_header) == 1 and pop_header[0].lower() in ["t", "t_tl"]:
                        continue
                    popmatch = bool(
                        re.search(
                            "(^t_)|population|both|total|totl|proj|pop|ensemble",
                            header,
                            re.IGNORECASE,
                        )
                    )
                    if header.lower() in ["t", "t_tl"]:
                        pop_header = [header]
                        continue
                    sexyearmatch = bool(
                        re.search("_f|_m|m_|f_|year|female|male|trans", header, re.IGNORECASE)
                    )
                    agematch = bool(
                        re.search("^\d{1,2}\D|(\D\d{1,2}\D)|(\D\d$)", header, re.IGNORECASE)
                    )
                    agewordmatch = bool(re.search("(age|adult|plus)", header, re.IGNORECASE))
                    urmatch = bool(re.search("(urban|rural)", header, re.IGNORECASE))
                    if (
                        popmatch
                        and not sexyearmatch
                        and not agematch
                        and not agewordmatch
                        and not urmatch
                    ):
                        pop_header.append(header)

                if len(pcode_header) > 0:
                    row[4] = ", ".join(list(set(pcode_header)))

                if len(name_header) > 0:
                    row[5] = ", ".join(list(set(name_header)))

                if len(pop_header) == 1:
                    row[6] = pop_header[0]

                if len(pop_header) > 1:
                    totmatches = [bool(re.search("(total|totl)", header, re.IGNORECASE)) for header in pop_header]
                    if sum(totmatches) == 1:
                        row[6] = pop_header[totmatches.index(True)]
                        writer.writerow(row)
                        continue
                    yearmatches = [re.findall("(?<!\d)\d{4}(?!\d)", header, re.IGNORECASE) for header in pop_header]
                    yearmatches = sum(yearmatches, [])
                    if len(yearmatches) == 0:
                        logger.info(f"Not sure which header to pick: {pop_header}")
                        row[6] = ",".join(pop_header)
                        writer.writerow(row)
                        continue
                    yearmatches = [int(y) for y in yearmatches]
                    maxyear = [h for h in pop_header if str(max(yearmatches)) in h]
                    if len(maxyear) != 1:
                        logger.info(f"Not sure which header to pick: {pop_header}")
                        row[6] = ",".join(pop_header)
                        writer.writerow(row)
                        continue
                    row[6] = maxyear[0]

                writer.writerow(row)

    logger.info("Wrote out population headers")
    return
