import asyncio
import logging
import time
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime
from traceback import format_tb
from urllib.parse import urlparse

import aiohttp
import pandas as pd

logger = logging.getLogger("heureka")


def process_product(product_json):
    """
    extracts product level data from json response
    """
    product = {
        f"product_{k}": v
        for k, v in product_json.items()
        if k
        in {
            "id",
            "name",
            "slug",
            "min_price",
            "url",
            "status",
            "rating",
            "category",
            "producer",
            "top_shop",
            "category_position",
            "offer_attributes",
            "images",
        }
    }
    if (product_rating := product.get("product_rating")) is not None:
        product["product_rating_rating"] = product_rating.get("rating")
        product["product_rating_review_count"] = product_rating.get("review_count")
    return product


def process_offer(offer):
    """
    extracts offer details for each shop from json response
    """
    offer_items = {"offer_" + k: v for k, v in offer.items()}
    offer_items["offer_availability_type"] = offer_items.get(
        "offer_availability", {}
    ).get("type")
    offer_items["offer_availability_in_stock"] = (
        1 if offer_items["offer_availability_type"] == "IN_STOCK" else 0
    )
    return offer_items


def process_shop(shop, index, index_name):
    """
    extracts shop level data from json response
    """
    shop_items = {"shop_" + k: v for k, v in shop.items() if k != "offers"}
    shop_items[index_name] = index
    shop_with_offers = [
        {**shop_items, **process_offer(offer)} for offer in shop["offers"]
    ]
    return shop_with_offers


def process_response(response_json):
    """
    combines processing of product, shop and offer level data
    """

    try:
        response_content = response_json["result"]["product"]
    except Exception as e:
        logger.debug("Response does not contain product data.")
        logger.debug(f"Exception {e}")
        logger.debug(response_json)
        return None

    if response_content is None:
        return None

    else:
        try:
            product = process_product(response_content)

            shops_with_positions = [
                process_shop(shop, position, "position")
                for position, shop in enumerate(response_content["shops"], start=1)
            ]

            shops_with_positions_flat = [
                item for sublist in shops_with_positions for item in sublist
            ]

            highlighted_shops_with_positions = [
                process_shop(shop, position, "highlighted_position")
                for position, shop in enumerate(
                    response_content["highlighted_shops"], start=1
                )
            ]

            highlighted_shops_with_positions_flat = [
                item for sublist in highlighted_shops_with_positions for item in sublist
            ]

            all_shops = (
                shops_with_positions_flat + highlighted_shops_with_positions_flat
            )
            result = [{**product, **shop} for shop in all_shops]
            return result

        except Exception as e:
            logger.error(e)
            return None


def batches(product_list, batch_size, window_size, sleep_time=5):
    window_start = time.monotonic()
    while product_list:
        batch = product_list[:batch_size]
        del product_list[:batch_size]

        while time.monotonic() - window_start < window_size:
            logger.debug("waiting for time window to expire...")
            time.sleep(sleep_time)
        window_start = time.monotonic()
        yield batch


@contextmanager
def time_logger():
    start = time.monotonic()
    try:
        yield
    finally:
        logger.info(f"Duration: {round(time.monotonic() - start)} seconds")


async def fetch_one(product_results, client, url, key, product_id, language):
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "product.get",
        "params": {"language": language, "access_key": key, "id": product_id},
    }

    async with await client.post(url, json=payload) as resp:
        resp = await resp.json()
        product_results.append(process_response(resp))


async def fetch_batch(product_list, api_url, api_key, language):
    product_results = []
    async with aiohttp.ClientSession() as client:
        tasks = [
            asyncio.create_task(
                fetch_one(
                    product_results, client, api_url, api_key, product_id, language
                )
            )
            for product_id in product_list
        ]
        await asyncio.gather(*tasks, return_exceptions=True)

    return product_results


def load_full_material_map(datadir, material_mapping_filename, country):
    # read unique product ids
    full_material_map = pd.read_csv(
        f"{datadir}in/tables/{material_mapping_filename}.csv", dtype=str
    )

    full_material_map = full_material_map[
        (full_material_map["country"] == country)
        & (full_material_map["source"] == "heureka")
        & (full_material_map["material"] != "")
        & pd.notnull(full_material_map["material"])
        & (full_material_map["cse_id"] != "")
        & pd.notnull(full_material_map["cse_id"])
    ]
    return full_material_map


def load_additional_top_products(datadir, top_products_filename, country, material_map):
    top_products = pd.read_csv(
        f"{datadir}in/tables/{top_products_filename}.csv", dtype=str
    )
    top_products = top_products[
        (top_products["country"] == country)
        & (top_products["id"] != "")
        & pd.notnull(top_products["id"])
    ]
    top_products = top_products.rename(columns={"id": "cse_id"})

    mapped_materials = set(material_map["cse_id"].unique())
    top_products = top_products[~top_products["cse_id"].isin(mapped_materials)]
    top_products["source"] = "heureka"
    top_products["material"] = "no_internal_id"
    top_products["distrchan"] = "no_internal_id"
    return top_products[["material", "cse_id", "country", "distrchan", "source"]]


def load_todays_runs_history(datadir, runs_history_filename):
    runs_history = pd.read_csv(
        f"{datadir}in/tables/{runs_history_filename}.csv", parse_dates=["DATETIME"]
    )
    runs_today = runs_history[
        runs_history["DATETIME"].dt.date == datetime.utcnow().date()
    ]
    return runs_today


def load_hourly_material_map(datadir, hourly_materials_filename, cse_material_map):
    hourly_materials_df = pd.read_csv(
        f"{datadir}in/tables/{hourly_materials_filename}.csv", dtype=str
    )
    hourly_materials = set(hourly_materials_df["MATERIAL"].unique())
    hourly_material_map = cse_material_map[
        cse_material_map["material"].isin(hourly_materials)
    ]
    return hourly_material_map


def decide_run_type(runs_today_history, first_daily_load_utc_hour):
    run_type = "HOURLY"
    if ("DAILY" not in runs_today_history["RUN_TYPE"].unique()) and (
        datetime.utcnow().hour >= first_daily_load_utc_hour
    ):
        run_type = "DAILY"
    logger.info(f"{run_type} load shall be executed.")
    return run_type


def process_batch_output(
    batch_results, material_dictionary, naming_map, out_cols, country
):
    output = pd.DataFrame(batch_results, dtype=str)
    # take just one offer per eshop for a given product
    # later, we might want to select non-randomly
    # the process_response() function outputs rows with highlighted position and position separately
    # pandas first selects first non-null value
    # this also collapses the info in position and highlighted position to one row
    logger.info("Deduplicating batch.")
    # FUNKY ERROR STARTED HERE
    success_ids = set(output["product_id"].astype("int64").unique())
    start = datetime.utcnow()
    output = output.groupby(["product_id", "shop_id"], as_index=False).first()
    logger.debug(f"DataFrame groupby time: {datetime.utcnow() - start}")
    # FUNKY ERROR DIT NOT GET HERE
    logger.info("Extracting eshop names.")
    start = datetime.utcnow()
    output["ESHOP"] = (
        output["shop_homepage"]
        .map(lambda x: urlparse(x).netloc)
        .str.replace(r"(www.)", r"", regex=True)
        .str.lower()
    )
    logger.debug(f"ESHOP url parse time: {datetime.utcnow() - start}")

    output["URL"] = (
        "https://heureka."
        + country.lower()
        + "/exit/"
        + output["shop_slug"].astype(str)
        + "/"
        + output["offer_id"]
    )
    # no need to merge on country as the loop runs only for one country
    logger.info("Merging batch with material map.")
    start = datetime.utcnow()
    # first, rename columns
    output = output.rename(columns=naming_map)
    # second, get rid of columns that are not used
    output = output[output.columns.intersection(out_cols)]
    # third, merge dataframes
    output = pd.merge(
        output,
        material_dictionary[["material", "distrchan", "cse_id"]].rename(
            columns=str.upper
        ),
        how="inner",
        left_on=["CSE_ID"],
        right_on=["CSE_ID"],
    ).fillna("")
    logger.debug(f"Merge output-material_dict time: {datetime.utcnow() - start}")
    start = datetime.utcnow()
    # use only columns that are needed and exist in dataframe
    output = output[output.columns.intersection(out_cols)].to_dict(orient="records")
    logger.debug(f"output.to_dict time: {datetime.utcnow() - start}")
    return output, success_ids


def save_runs_history(**kwargs):
    logger.info("Saving runs history")
    run_log = pd.DataFrame(
        {
            "DATETIME": [kwargs["utctime_started"]],
            "RUN_TYPE": [kwargs["run_type"]],
            "SUCCEEDED_COUNT": [len(kwargs["written_ids"])],
            "FAILED_COUNT": [len(kwargs["missing_products"])],
        }
    )
    run_log["DATETIME"] = pd.to_datetime(run_log["DATETIME"])
    runs_history = pd.concat([kwargs["runs_today"], run_log])
    runs_history.to_csv(
        f'{kwargs["datadir"]}out/tables/{kwargs["runs_history_filename"]}.csv',
        index=False,
    )


class HeurekaProducer:
    def __init__(self, task_queue, datadir, parameters):
        self.task_queue = task_queue
        self.datadir = datadir
        self.parameters = parameters
        self.colnames = [
            "AVAILABILITY",
            "COUNTRY",
            "CSE_ID",
            "CSE_URL",
            "DISTRCHAN",
            "ESHOP",
            "FREQ",
            "HIGHLIGHTED_POSITION",
            "MATERIAL",
            "POSITION",
            "PRICE",
            "RATING",
            "REVIEW_COUNT",
            "SOURCE",
            "SOURCE_ID",
            "STOCK",
            "TOP",
            "TS",
            "URL",
            "PRODUCER",
            "PRODUCT_NAME",
            "CATEGORY",
        ]

    def produce(self):
        try:
            utctime_started = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            utctime_started_short = datetime.utcnow().strftime("%Y%m%d%H%M%S")

            kbc_datadir = self.datadir
            parameters = self.parameters
            logger.info("Extracting parameters from config.")

            cse_material_mapping_filename = parameters.get(
                "cse_material_mapping_filename"
            )
            top_products_filename = parameters.get("top_products_filename")
            columns_mapping = parameters.get("columns_mapping")
            api_key = parameters.get("#api_key")
            countries_to_scrape = parameters.get("countries_to_scrape")
            # log parameters (excluding sensitive designated by '#')
            logger.info({k: v for k, v in parameters.items() if "#" not in k})

            for country in countries_to_scrape:
                logger.info(f"Running scraper for country: {country}")
                logger.info(parameters.get(country))
                api_url = parameters.get(country).get("api_url")
                language = parameters.get(country).get("language")
                hourly_materials_filename = parameters.get(country).get(
                    "hourly_materials_filename"
                )
                runs_history_filename = parameters.get(country).get(
                    "runs_history_filename"
                )
                top_products_load_day = parameters.get(country).get(
                    "top_products_load_day"
                )
                batch_size = parameters.get(country).get("batch_size", 2490)
                time_window_per_batch = parameters.get(country).get(
                    "time_window_per_batch", 16
                )
                max_attempts = parameters.get(country).get("max_attempts", 1)
                first_daily_load_utc_hour = int(
                    parameters.get(country).get("first_daily_load_utc_hour", 3)
                )

                # decide run_type
                logger.info("Loading runs history.")
                runs_today = load_todays_runs_history(
                    kbc_datadir, runs_history_filename
                )
                run_type = decide_run_type(runs_today, first_daily_load_utc_hour)
                logger.info("Loading materials map.")
                cse_material_map = load_full_material_map(
                    kbc_datadir, cse_material_mapping_filename, country
                )
                if top_products_load_day is not None:
                    top_products_load_day_int = int(top_products_load_day)
                    if datetime.utcnow().weekday() == top_products_load_day_int:
                        logger.info("Adding top products to dict.")
                        top_products = load_additional_top_products(
                            kbc_datadir,
                            top_products_filename,
                            country,
                            cse_material_map,
                        )
                        cse_material_map = pd.concat(
                            [cse_material_map, top_products], sort=True
                        )

                if run_type == "HOURLY":
                    cse_material_map = load_hourly_material_map(
                        kbc_datadir, hourly_materials_filename, cse_material_map,
                    )

                original_product_ids = set(cse_material_map["cse_id"].astype("int64"))
                product_ids = list(original_product_ids)

                logger.info(f"Input unique products: {len(original_product_ids)}")
                logger.info(f"product_ids sample: {product_ids[:5]}")

                attempts = defaultdict(int)
                written_ids = set()

                with time_logger():
                    for batch_i, product_batch in enumerate(
                        batches(
                            product_ids,
                            batch_size=batch_size,
                            window_size=time_window_per_batch,
                        )
                    ):
                        logger.debug(f"Starting timing for batch {batch_i}")
                        batch_start = datetime.utcnow()
                        for pid in product_batch:
                            attempts[pid] += 1

                        logger.info(f"Scraping batch {batch_i}")
                        start = datetime.utcnow()
                        result_list = asyncio.run(
                            fetch_batch(
                                product_list=product_batch,
                                api_url=api_url,
                                api_key=api_key,
                                language=language,
                            )
                        )
                        logger.debug(f"Scraping time: {datetime.utcnow() - start}")
                        logger.info(f"Scraped batch {batch_i}")
                        # flatten and transform results
                        batch_results = [
                            # filter item columns to only relevant ones and add utctime_started
                            {
                                **{colname: colval for colname, colval in item.items()},
                                **{
                                    "TS": utctime_started,
                                    "SOURCE": "heureka",
                                    "SOURCE_ID": f"heureka_{country}_{utctime_started_short}",
                                    "FREQ": "d",
                                    "COUNTRY": country,
                                },
                            }
                            for sublist in result_list
                            if sublist
                            for item in sublist
                        ]
                        logger.info(f"Parsed batch {batch_i}")
                        if not batch_results:
                            logger.warning(f"Batch {batch_i} if empty!")
                            continue

                        start = datetime.utcnow()
                        batch_output, success_ids = process_batch_output(
                            batch_results,
                            material_dictionary=cse_material_map,
                            naming_map=columns_mapping,
                            out_cols=self.colnames,
                            country=country,
                        )
                        logger.debug(
                            f"process_batch_output time: {datetime.utcnow() - start}"
                        )
                        failed_ids = set(product_batch).difference(success_ids)

                        if max_attempts > 1:
                            failed_under_max_attempts = [
                                pid
                                for pid in failed_ids
                                if attempts[pid] < max_attempts
                            ]
                            product_ids.extend(failed_under_max_attempts)
                        else:
                            failed_under_max_attempts = []

                        logger.debug(
                            f"BATCH TIME time: {datetime.utcnow() - batch_start}"
                        )
                        logger.debug(f"End of timing for batch {batch_i}")
                        logger.info(f"{len(success_ids)} IDs retrieved successfully")
                        logger.info(f"{len(failed_ids)} IDs failed")
                        logger.info(
                            f"{len(failed_under_max_attempts)} IDs requeued for extraction"
                        )
                        logger.info(f"Queueing batch {batch_i}")
                        self.task_queue.put(batch_output)
                        written_ids = written_ids.union(success_ids)

                    missing_products = list(original_product_ids - written_ids)
                    logger.info(f"Output unique products #: {len(written_ids)}")
                    logger.info(f"Missing product #: {len(missing_products)}")
                    start = datetime.utcnow()
                    save_runs_history(
                        datadir=kbc_datadir,
                        utctime_started=utctime_started,
                        run_type=run_type,
                        missing_products=missing_products,
                        written_ids=written_ids,
                        runs_today=runs_today,
                        runs_history_filename=runs_history_filename,
                    )
                    logger.debug(
                        f"merging batch_results time: {datetime.utcnow() - start}"
                    )

            logger.info("Producer completed. Putting DONE to queue.")
        except Exception as e:
            trace = "Traceback:\n" + "".join(format_tb(e.__traceback__))
            logger.error(f"Error occurred {e}", extra={"full_message": trace})
        finally:
            self.task_queue.put("DONE")
