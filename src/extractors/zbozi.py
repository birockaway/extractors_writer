import json
import logging
import time
from datetime import datetime, timedelta
from traceback import format_tb

import numpy as np
import pandas as pd
import requests

logger = logging.getLogger("zbozi")
logger.setLevel(logging.DEBUG)

TIMEOUT_DELTA = timedelta(hours=4, minutes=30)
EXECUTION_START = datetime.utcnow()
URL_BASE = "https://api.zbozi.cz"
BATCH_SIZE_PRODUCTS = 10
BATCH_SIZE_SHOPS = 100
TS = EXECUTION_START.strftime("%Y-%m-%d %H:%M:%S")
CURRENT_DATE_STR = EXECUTION_START.strftime("%Y-%m-%d")
COUNTRY = "CZ"
DISTRCHAN = "MA"
SOURCE = "zbozi"
FREQ = "d"
SOURCE_ID = f"{SOURCE}_{TS}"


class ZboziProducer(object):
    def __init__(self, pipeline, datadir, parameters):
        self.task_queue = pipeline
        self.datadir = datadir
        self.login = parameters.get("#login")
        self.password = parameters.get("#password")
        self.out_cols = [
            "AVAILABILITY",
            "COUNTRY",
            "CATEGORY",
            "CSE_ID",
            "CSE_URL",
            "DISTRCHAN",
            "ESHOP",
            "FREQ",
            "HIGHLIGHTED_POSITION",
            "MATERIAL",
            "POSITION",
            "PRICE",
            "PRODUCER",
            "PRODUCT_NAME",
            "RATING",
            "REVIEW_COUNT",
            "SOURCE",
            "SOURCE_ID",
            "STOCK",
            "TOP",
            "TS",
            "URL",
        ]
        self.all_cols = self.out_cols + ["DATE", "ZBOZI_SHOP_ID", "MATCHING_ID"]
        self.export_table = "results"
        self.daily_uploads_file = "zbozi_products.csv"
        self.previous_df = self.load_previous_ids()
        logger.info(f"Loaded {len(self.previous_df)} previous material CSE_IDs")

        try:
            # load next url from file, if previous run ended early
            keep_scraping = pd.read_csv(
                f"{self.datadir}in/tables/keep_scraping.csv", dtype=object
            )
            logger.debug(str(keep_scraping))
            next_url = keep_scraping.iloc[0, 0]
            if next_url and str(next_url).lower() not in ["none", "nan", "false"]:
                self.next_url = next_url
            else:
                raise IndexError()

        except (IndexError, FileNotFoundError):
            logger.warning("No next_url, starting from scratch")
            self.next_url = (
                "/v1/shop/items?paired=True&limit=1000&loadProductDetail=False"
            )

    @property
    def auth(self):
        return self.login, self.password

    def update_keep_scraping(self):
        if not self.next_url:
            logger.warning("Scraping finished, next run will start anew")
        pd.DataFrame({"KS": [self.next_url]}).to_csv(
            f"{self.datadir}/out/tables/keep_scraping.csv", index=False
        )

    def gen_seq_chunks(self, seq, chunk_size):
        for pos in range(0, len(seq), chunk_size):
            yield seq[pos : pos + chunk_size]

    def get_products(self, ids_str):
        try:
            response = requests.get(
                f"{URL_BASE}/v1/products/{ids_str}", auth=self.auth, timeout=120
            )
        except requests.exceptions.Timeout:
            logger.warning(f"Products request timed out")
            return None

        if not response or response.status_code // 100 != 2:
            logger.warning(
                f"failed products get request: {response.status_code, response.text}"
            )
            return None

        content = json.loads(response.text)
        product_pages = pd.DataFrame()
        try:
            for data in content["data"]:
                if not data.get("shopItems") and not data.get("topShopItems"):
                    # skip invalid shops
                    continue

                # cast all values to str so pandas will cast it to object arrays
                shops_df = pd.DataFrame(
                    {k: str(v) if v is not None else "" for k, v in d.items()}
                    for d in data.get("shopItems")
                )
                shops_df["POSITION"] = np.arange(1, len(shops_df) + 1)

                h_shops_df = pd.DataFrame(
                    {k: str(v) if v is not None else "" for k, v in d.items()}
                    for d in data.get("topShopItems")
                )
                h_shops_df["HIGHLIGHTED_POSITION"] = np.arange(1, len(h_shops_df) + 1)

                product_page = pd.concat([shops_df, h_shops_df], sort=True)
                product_page["DATE"] = CURRENT_DATE_STR
                product_page["TS"] = datetime.now().strftime("%Y%m%d%H%M%S")
                product_page["CSE_ID"] = str(data["productId"])
                product_page["CATEGORY"] = str(data["categoryId"])
                product_page["PRODUCT_NAME"] = str(data["productName"])
                product_page["PRODUCER"] = str(data['manufacturerId'])
                product_page = product_page.groupby("shopId", as_index=False).first()
                product_pages = pd.concat([product_pages, product_page], sort=True)

        except Exception as e:
            logger.warning(f"Exception: {e}, data: {str(content)}")
            return None
        else:
            return product_pages

    def get_shop_names(self, shop_ids_str):
        try:
            response = requests.get(
                f"{URL_BASE}/v1/shops/{shop_ids_str}", auth=self.auth, timeout=120
            )
        except requests.exceptions.Timeout:
            logger.warning(f"Shops request timed out")
            return None

        if response.status_code // 100 != 2:
            logger.warning(
                f"Shops request status not OK: {response.status_code, response.text}"
            )
            return None

        return [
            {k: str(v) if v is not None else "" for k, v in d.items()}
            for d in response.json()["data"]
        ]

    def write_output(self, products_df):
        products_df.reset_index(drop=True, inplace=True)
        to_send = products_df[self.out_cols].fillna("").to_dict("records")
        self.task_queue.put(to_send)

    def merge_tables(self, products_df, shop_names=None, material_map=None):
        if shop_names is not None:
            shops_df = pd.DataFrame(shop_names, columns=["name", "rating", "shopId"])
            shops_df.rename(
                columns={
                    "name": "ESHOP",
                    "rating": "RATING",
                    "shopId": "ZBOZI_SHOP_ID",
                    "availability": "AVAILABILITY",
                },
                inplace=True,
            )
            eshop_notnull_index = pd.notnull(products_df["ESHOP"])
            products_df_w_shop_info = products_df[eshop_notnull_index]
            products_df_wo_shop_info = products_df[~eshop_notnull_index]

            products_df_wo_shop_info = products_df_wo_shop_info.drop(
                ["ESHOP", "RATING"], axis=1
            )
            products_df = pd.merge(
                products_df_wo_shop_info, shops_df, on="ZBOZI_SHOP_ID", how="left"
            )
            products_df = pd.concat([products_df, products_df_w_shop_info], sort=True)

            products_df["ESHOP"] = products_df["ESHOP"].str.lower()

        if material_map is not None:
            mat_notnull_index = pd.notnull(products_df["MATERIAL"])
            products_df_w_mat_info = products_df[mat_notnull_index]
            products_df_wo_mat_info = products_df[~mat_notnull_index]

            products_df_wo_mat_info = products_df_wo_mat_info.drop("MATERIAL", axis=1)
            products_df = pd.merge(
                products_df_wo_mat_info, material_map, on="CSE_ID", how="left"
            )
            products_df = pd.concat([products_df, products_df_w_mat_info], sort=True)

        products_df = products_df.groupby(
            ["DATE", "CSE_ID", "ZBOZI_SHOP_ID"], as_index=False
        ).first()

        products_df["TS"] = TS
        products_df["COUNTRY"] = COUNTRY
        products_df["DISTRCHAN"] = DISTRCHAN
        products_df["SOURCE"] = SOURCE
        products_df["SOURCE_ID"] = SOURCE_ID
        products_df["FREQ"] = FREQ
        products_df.loc[:, "STOCK"] = np.where(
            products_df["AVAILABILITY"] == "0", "1", "0"
        )
        return products_df

    def load_previous_ids(self):
        try:
            df = pd.read_csv(
                f"{self.datadir}in/tables/{self.daily_uploads_file}", dtype=object
            )
            # keep only today's values
            df = df.drop(df[df["DATE"] != CURRENT_DATE_STR].index)
        except Exception:
            df = pd.DataFrame(columns=["CSE_ID", "DATE"])

        return df

    def produce(self):
        try:
            batch_counter = 0
            logger.debug(f'Starting run with source_id: "{SOURCE_ID}"')
            logger.debug(f"Start time: {EXECUTION_START}, timeout: {TIMEOUT_DELTA}")
            while (
                self.next_url is not None
                and datetime.utcnow() - EXECUTION_START < TIMEOUT_DELTA
            ):
                logger.info(f"Starting batch #{batch_counter}")
                rows_count = self.produce_batch()
                logger.info(
                    f"Finished batch #{batch_counter}, rows written: {rows_count}"
                )
                batch_counter += 1

        except Exception as e:
            trace = "Traceback:\n" + "".join(format_tb(e.__traceback__))
            logger.error(f"Error occurred {e}", extra={"full_message": trace})
        finally:
            # write url for next run to continue where this run left off
            self.update_keep_scraping()
            logger.info(f"Saving gathered CSE_IDs, {self.previous_df.shape}")
            # write out all gathered ids for deduplication
            self.previous_df.to_csv(
                f"{self.datadir}out/tables/{self.daily_uploads_file}", index=False
            )
            # send ending token
            self.task_queue.put("DONE")

    def produce_batch(self):
        try:
            response = requests.get(
                f"{URL_BASE}{self.next_url}", auth=self.auth, timeout=120
            )
        except requests.exceptions.Timeout:
            response = None

        while not response or response.status_code // 100 != 2:
            time.sleep(1.01)
            try:
                response = requests.get(
                    f"{URL_BASE}{self.next_url}", auth=self.auth, timeout=120
                )
            except requests.exceptions.Timeout:
                response = None

        content = json.loads(response.text)
        # set url for next run
        next_url = content.get("links", dict()).get("next")
        logger.info(f"Next request url: {next_url}")
        self.next_url = next_url
        content_data = content.get("data", list())
        logger.debug(f"Number of items: {len(content_data)}")
        product_ids = [
            (str(item["itemId"]), str(item["product"]["productId"]))
            for item in content_data
            if item.get("product") is not None
        ]

        logger.debug("End product requests")
        material_map = pd.DataFrame(product_ids, columns=["MATERIAL", "CSE_ID"]).astype(
            object
        )
        before_exclude = len(material_map)
        previous_ids = self.previous_df["CSE_ID"].to_list()
        # remove ids already scraped today
        excluded_ids = material_map["CSE_ID"].isin(previous_ids)
        logger.debug(
            f'Excluding from scrape: {material_map[excluded_ids]["CSE_ID"].values.tolist()}'
        )
        material_map = material_map[~excluded_ids]
        logger.info(
            f"Excluded {before_exclude - len(material_map)} materials from previous run"
        )

        #############################################################################
        # PRODUCT PAGES
        failed_product_ids_strs = list()
        for ids_group in self.gen_seq_chunks(
            material_map["CSE_ID"], BATCH_SIZE_PRODUCTS
        ):
            ids_str = ",".join(map(str, map(int, ids_group)))
            failed_product_ids_strs.append(ids_str)

        products_df = pd.DataFrame(columns=self.all_cols)
        logger.debug("Start product pages requests")

        while failed_product_ids_strs:
            ids_strs = failed_product_ids_strs[:]
            failed_product_ids_strs = list()
            for ids_str in ids_strs:
                product_batch_df = self.get_products(ids_str)
                if product_batch_df is not None:
                    product_batch_df = product_batch_df.rename(
                        columns={
                            "shopId": "ZBOZI_SHOP_ID",
                            "availability": "AVAILABILITY",
                            "matchingId": "MATCHING_ID",
                            "price": "PRICE",
                        }
                    )
                    products_df = pd.concat([products_df, product_batch_df], sort=True)
                    time.sleep(0.23)
                else:
                    logger.warning(f"product_ids_str: {ids_str} failed")
                    failed_product_ids_strs.append(ids_str)
                    time.sleep(1.21)

        logger.debug("End product pages requests")

        if products_df.empty:
            # no new data, no point in continuing, batch will write nothing
            return 0

        ###############################################################################
        # SHOP NAMES
        all_shop_names = list()
        remaining_shop_ids = products_df["ZBOZI_SHOP_ID"].unique().tolist()
        failed_shop_ids_strs = [
            ",".join(map(str, map(int, shop_ids_group)))
            for shop_ids_group in self.gen_seq_chunks(
                remaining_shop_ids, BATCH_SIZE_SHOPS
            )
        ]

        while failed_shop_ids_strs:
            shop_ids_strs = failed_shop_ids_strs[:]
            failed_shop_ids_strs = list()

            for shop_ids_str in shop_ids_strs:
                shop_names = self.get_shop_names(shop_ids_str)
                if shop_names is not None:
                    all_shop_names.extend(shop_names)
                    time.sleep(1.01)
                else:
                    failed_shop_ids_strs.append(shop_ids_str)
                    time.sleep(1)

        products_df = self.merge_tables(
            products_df, shop_names=all_shop_names, material_map=material_map
        )
        self.write_output(products_df)

        # write out
        new_prevs_df = (
            products_df[["CSE_ID", "DATE"]]
            .groupby(["CSE_ID", "DATE"])
            .first()
            .reset_index()
        )
        self.previous_df = pd.concat([self.previous_df, new_prevs_df])
        return len(products_df)
