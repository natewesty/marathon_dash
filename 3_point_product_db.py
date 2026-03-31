# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "gspread>=6.2.1",
#     "marimo>=0.20.2",
#     "polars>=1.39.3",
#     "protobuf>=7.34.1",
#     "python-dotenv>=1.2.2",
#     "pyzmq>=27.1.0",
#     "requests>=2.33.0",
#     "shopifyapi>=12.7.0",
# ]
# ///

import marimo

__generated_with = "0.20.4"
app = marimo.App()


@app.cell
def _():
    import os
    import json
    import requests
    import gspread
    import shopify
    import marimo as mo
    import polars as pl
    from dotenv import load_dotenv, find_dotenv
    from google.oauth2.service_account import Credentials
    from pathlib import Path

    loaded = load_dotenv(dotenv_path=Path(__file__).parent.parent / '.env')

    print(f"Env Loaded: {loaded}")

    print(os.getenv("COOL_CHILI_VAR"))

    # Linnworks - PROD - Variables
    application_id = os.getenv("LINNWORKS_PROD_APPLICATION_ID")
    application_secret = os.getenv("LINNWORKS_PROD_APPLICATION_SECRET")
    token = os.getenv("LINNWORKS_PROD_TOKEN")

    start_url = "https://api.linnworks.net/api/"

    # GSheets - PROD - Variables/Scopes
    creds_dict = {
        "type": "service_account",
        "project_id": os.environ.get("GOOGLE_PROJECT_ID"),
        "private_key_id": os.environ.get("GOOGLE_PRIVATE_KEY_ID"),
        "private_key": os.environ.get("GOOGLE_PRIVATE_KEY").replace("\\n", "\n"),
        "client_email": os.environ.get("GOOGLE_CLIENT_EMAIL"),
        "client_id": os.environ.get("GOOGLE_CLIENT_ID"),
        "auth_uri": os.environ.get("GOOGLE_AUTH_URI"),
        "token_uri": os.environ.get("GOOGLE_TOKEN_URI"),
    }

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    # LBW Shopify - PROD - Variables
    shop_url = os.getenv("LBW_SHOP_URL")
    api_version = os.getenv("LBW_API_VERSION")
    access_token = os.getenv("LBW_ACCESS_TOKEN")
    return (
        Credentials,
        access_token,
        api_version,
        application_id,
        application_secret,
        creds_dict,
        gspread,
        json,
        mo,
        pl,
        requests,
        scopes,
        shop_url,
        shopify,
        start_url,
        token,
    )


@app.cell
def _(mo):
    mo.md(r"""
    ##Config
    """)
    return


@app.cell
def _(
    Credentials,
    application_id,
    application_secret,
    creds_dict,
    gspread,
    requests,
    scopes,
    start_url,
    token,
):
    # Linnworks - Auth Handshake
    auth_url = f"{start_url}Auth/AuthorizeByApplication" #<--- Must have a pre-existing app in Linnworks in order to utilze this endpoint

    payload = {
        "ApplicationId": application_id,
        "ApplicationSecret": application_secret,
        "Token": token
    }

    base_headers = {
        "accept": "application/json",
        "Content-Type": "application/json"
    }

    linn_shake_response = requests.post(auth_url, headers=base_headers, json=payload)
    linn_shake_response.raise_for_status()

    session_token = linn_shake_response.json()["Token"]
    session_base_url = f"https://{linn_shake_response.json()["Locality"]}-ext.linnworks.net/api/"

    # GSheets - Credentials Fetch & Sheet Sourcing
    gcreds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    client = gspread.authorize(gcreds)

    sheet = client.open("Spring Marathon 2026").worksheet("Combined List")
    records = sheet.get_all_records(head=1, expected_headers=[])
    return records, session_base_url, session_token


@app.cell
def _(pl, records):
    df_gsheet = pl.DataFrame(records).slice(1)
    df_gsheet_clean = df_gsheet.drop(["Built In Shopify", "Built In Linnworks", "Marathon Tag", "Published", "Tabs", "Dupe Check","Issues"])
    return (df_gsheet_clean,)


@app.cell
def _():
    get_full_scope_product = """
    query GetProductWithVariantsAndMetafields(
      $id: ID!
      $productMetafieldKeys: [String!]
    ) {
      product(id: $id) {
        id
        title
        # Product-level metafields
        metafields(first: 10,keys: $productMetafieldKeys) {
          edges {
            node {
              key
              value
            }
          }
        }
        # Variants
        variants(first: 50) {
          edges {
            node {
              sku
              price
              inventoryQuantity
              inventoryItem {
                unitCost{
                    amount
                }
                measurement {
                  weight {
                    value
                  }
                }
            }
        }
      }
    }
    }
    }
    """

    souce_gid = """
    query GetProductBySku($sku: String!) {
      productVariants(first: 1, query: $sku) {
        edges {
          node {
            id
            sku
            title
            product {
              id
              title
              handle
            }
          }
        }
      }
    }
    """
    return get_full_scope_product, souce_gid


@app.cell
def _(
    access_token,
    api_version,
    df_gsheet_clean,
    json,
    shop_url,
    shopify,
    souce_gid,
):
    gid_query_list = []
    sku_list = []

    for shop_sku in df_gsheet_clean["SKU"]:
            shop_query = souce_gid
            shop_vars = {"sku": f"sku:{shop_sku}"}
            with shopify.Session.temp(shop_url, api_version, access_token):
                shop_response = json.loads(shopify.GraphQL().execute(shop_query, shop_vars))
                gid_query_list.append(shop_response["data"]["productVariants"]["edges"][0]["node"]["product"]["id"])
                sku_list.append(shop_sku)
    return gid_query_list, sku_list


@app.cell
def _(
    access_token,
    api_version,
    get_full_scope_product,
    gid_query_list,
    json,
    pl,
    shop_url,
    shopify,
):
    def parse_product(product: dict) -> dict:
        # Flatten metafields from edge/node structure into a simple dict
        metafields = {
            node["node"]["key"]: node["node"]["value"]
            for node in product["metafields"]["edges"]
        }

        # Variants are also edge/node — grab the first (and typically only) one
        variant = product["variants"]["edges"][0]["node"]

        return {
            "Title": product["title"],
            "SKU": variant["sku"],
            "Bottle Size": metafields.get("seed.bottle_size"),
            "Alcohol": metafields.get("seed.alcohol"),
            "Weight": variant["inventoryItem"]["measurement"]["weight"]["value"],
            "Carrier Alc": metafields.get("linnworks.carrier_alcohol_declaration"),
            "FedEx": metafields.get("linnworks.fedex_signature"),
            "Cost": variant["inventoryItem"]["unitCost"]["amount"],
            "Price": variant["price"],
            "Inv Quant": variant["inventoryQuantity"],
        }


    shop_rows = []

    with shopify.Session.temp(shop_url, api_version, access_token):
        for gid in gid_query_list:
            query = get_full_scope_product
            vars = {
                "id": gid,
                "productMetafieldKeys": [
                    "seed.bottle_size",
                    "seed.alcohol",
                    "seed.carrier_alcohol_declaration",
                    "linnworks.carrier_alcohol_declaration",
                    "linnworks.fedex_signature",
                ],
            }
            response = json.loads(shopify.GraphQL().execute(query, vars))
            shop_rows.append(parse_product(response["data"]["product"]))

    df_shopify = pl.DataFrame(shop_rows)
    return (df_shopify,)


@app.cell
def _(pl, requests, session_base_url, session_token, sku_list):
    # Define Standard Headers - Linnworks
    std_headers = {"accept": "application/json", "Authorization": session_token}

    EXT_PROP_MAP = {
        "Metafield [linnworks] - [carrier_alcohol_declarati": "Carrier Alc",
        "Metafield [linnworks] - [fedex_signature]": "FedEx",
        "Metafield [seed] - [alcohol]": "Alcohol",
        "Metafield [seed] - [bottle_size]": "Bottle Size",
    }

    def get_stock_item_ids(skus: list[str]) -> dict[str, str]:
        """Returns {sku: stock_item_id}, chunked to respect API limits"""
        results = {}
        chunk_size = 200
        for i in range(0, len(skus), chunk_size):
            chunk = skus[i:i + chunk_size]
            response = requests.post(
                f"{session_base_url}Inventory/GetStockItemIdsBySKU",
                headers={**std_headers, "Content-Type": "application/json"},
                json={"request": {"skus": chunk}}
            )
            response.raise_for_status()
            for item in response.json()["Items"]:
                results[item["SKU"]] = item["StockItemId"]
        return results


    def get_inventory_items(stock_item_ids: list[str]) -> dict[str, dict]:
        """Returns {stock_item_id: item_dict}, chunked to respect 200-item API limit"""
        results = {}
        chunk_size = 200
        for i in range(0, len(stock_item_ids), chunk_size):
            chunk = stock_item_ids[i:i + chunk_size]
            response = requests.post(
                f"{session_base_url}Stock/GetStockItemsFullByIds",
                headers={**std_headers, "Content-Type": "application/json"},
                json={"request": {"stockItemIds": chunk}}
            )
            response.raise_for_status()
            for item in response.json()["StockItemsFullExtended"]:
                results[item["StockItemId"]] = item
        return results


    def get_extended_properties(stock_item_id: str) -> dict[str, str]:
        """Returns {property_name: value} for a single item"""
        response = requests.post(
            f"{session_base_url}Inventory/GetInventoryItemExtendedProperties",
            headers=std_headers,
            data={"inventoryItemId": stock_item_id}
        )
        response.raise_for_status()
        return {prop["ProperyName"]: prop["PropertyValue"] for prop in response.json()}


    def get_stock_levels(stock_item_ids: list[str]) -> dict[str, int]:
        """Returns {stock_item_id: StockLevel} for LBW WMS location only"""
        results = {}
        chunk_size = 200
        for i in range(0, len(stock_item_ids), chunk_size):
            chunk = stock_item_ids[i:i + chunk_size]
            response = requests.post(
                f"{session_base_url}Stock/GetStockLevel_Batch",
                headers={**std_headers, "Content-Type": "application/json"},
                json={"request": {"stockItemIds": chunk}}
            )
            response.raise_for_status()
            for item in response.json():
                lbw_location = next(
                    (loc for loc in item.get("StockItemLevels", [])
                     if loc["Location"]["LocationName"] == "LBW WMS"),
                    None
                )
                results[item["pkStockItemId"]] = lbw_location.get("StockLevel", 0) if lbw_location else 0
        return results


    def parse_linnworks_item(sku: str, stock_item_id: str, item: dict, ext_props: dict, stock_levels: dict) -> dict:
        return {
            "Title": item.get("ItemTitle"),
            "SKU": sku,
            "Bottle Size": ext_props.get("Metafield [seed] - [bottle_size]"),
            "Alcohol": ext_props.get("Metafield [seed] - [alcohol]"),
            "Weight": item.get("Weight"),
            "Carrier Alc": ext_props.get("Metafield [linnworks] - [carrier_alcohol_declarati"),
            "FedEx": ext_props.get("Metafield [linnworks] - [fedex_signature]"),
            "Cost": item.get("PurchasePrice"),
            "Price": item.get("RetailPrice"),
            "Inv Quant": stock_levels.get(stock_item_id, 0),
        }


    # --- Main loop ---
    sku_to_id = get_stock_item_ids(sku_list)
    stock_item_ids = list(sku_to_id.values())
    id_to_sku = {v: k for k, v in sku_to_id.items()}

    inventory_items = get_inventory_items(stock_item_ids)
    stock_levels = get_stock_levels(stock_item_ids)

    linn_rows = []
    for stock_item_id, item in inventory_items.items():
        sku = id_to_sku[stock_item_id]
        ext_props = get_extended_properties(stock_item_id)
        linn_rows.append(parse_linnworks_item(sku, stock_item_id, item, ext_props, stock_levels))

    df_linnworks = pl.DataFrame(linn_rows)
    return (df_linnworks,)


@app.cell
def _(df_gsheet_clean, df_linnworks, df_shopify, pl):
    COMPARE_FIELDS = ["Title", "Bottle Size", "Alcohol", "Weight", "Carrier Alc", "FedEx", "Cost", "Price"]

    df_gs = df_gsheet_clean.select(["SKU"] + COMPARE_FIELDS).rename(
        {col: f"{col}_gs" for col in COMPARE_FIELDS}
    )
    df_sh = df_shopify.select(["SKU"] + COMPARE_FIELDS).rename(
        {col: f"{col}_sh" for col in COMPARE_FIELDS}
    )
    df_lw = df_linnworks.select(["SKU"] + COMPARE_FIELDS + ["Inv Quant"]).rename(
        {col: f"{col}_lw" for col in COMPARE_FIELDS + ["Inv Quant"]}
    )

    df_merged = (
        df_gs
        .join(df_sh, on="SKU", how="full", coalesce=True)
        .join(df_lw, on="SKU", how="full", coalesce=True)
    )

    # Flag discrepancies per field
    discrepancy_cols = []
    for comp_field in COMPARE_FIELDS:
        gs_col = pl.col(f"{comp_field}_gs")  # was f"{field}_gs"
        sh_col = pl.col(f"{comp_field}_sh")  # was f"{field}_sh"
        lw_col = pl.col(f"{comp_field}_lw")  # was f"{comp_field}_lw"
        if comp_field in ["Price", "Cost", "Weight", "Alcohol"]:
            gs_col = gs_col.cast(pl.Float64, strict=False)
            sh_col = sh_col.cast(pl.Float64, strict=False)
            lw_col = lw_col.cast(pl.Float64, strict=False)
        else:
            gs_col = gs_col.cast(pl.Utf8)
            sh_col = sh_col.cast(pl.Utf8)
            lw_col = lw_col.cast(pl.Utf8)
        discrepancy_cols.append(
            ((gs_col != sh_col) | (gs_col != lw_col) | (sh_col != lw_col)).alias(f"{comp_field}_mismatch")
        )

    df_discrepancies = df_merged.with_columns(discrepancy_cols)

    summary = {
        comp_field: df_discrepancies[f"{comp_field}_mismatch"].sum()
        for comp_field in COMPARE_FIELDS
    }
    total_skus = len(df_merged)
    print(f"Total SKUs: {total_skus}")
    print(summary)
    return (df_discrepancies,)


@app.cell
def _(df_discrepancies, mo, pl):
    # --- Aggregated view ---
    FIELDS = ["Title", "Bottle Size", "Alcohol", "Weight", "Carrier Alc", "FedEx", "Cost", "Price"]

    summary_data = {
        field: df_discrepancies[f"{field}_mismatch"].sum()
        for field in FIELDS
    }

    agg_rows = sorted(
        [{"Field": k, "Mismatches": int(v), "Pct": round(int(v) / len(df_discrepancies) * 100, 1)}
         for k, v in summary_data.items()],
        key=lambda x: x["Mismatches"],
        reverse=True
    )

    agg_table = mo.ui.table(
        pl.DataFrame(agg_rows),
        label="Discrepancies by field"
    )

    # --- SKU-level view ---
    FIELDS_TO_EXPORT = ["Title", "Bottle Size", "Alcohol", "Weight", "Carrier Alc", "FedEx", "Cost", "Price"]

    sku_rows = []
    for row in df_discrepancies.iter_rows(named=True):
        for field in FIELDS_TO_EXPORT:
            if row.get(f"{field}_mismatch"):
                gs = str(row.get(f"{field}_gs") or "")
                sh = str(row.get(f"{field}_sh") or "")
                lw = str(row.get(f"{field}_lw") or "")
                try:
                    gs_f, sh_f, lw_f = float(gs), float(sh), float(lw)
                    pattern = (
                        "GS + LW agree, Shopify differs" if gs_f == lw_f else
                        "Shopify + LW agree, GS differs" if sh_f == lw_f else
                        "GS + Shopify agree, LW differs" if gs_f == sh_f else
                        "All three differ"
                    )
                except ValueError:
                    pattern = (
                        "GS + LW agree, Shopify differs" if gs == lw else
                        "Shopify + LW agree, GS differs" if sh == lw else
                        "GS + Shopify agree, LW differs" if gs == sh else
                        "All three differ"
                    )
                sku_rows.append({
                    "SKU": row["SKU"],
                    "Field": field,
                    "GS": gs,
                    "Shopify": sh,
                    "Linnworks": lw,
                    "Pattern": pattern,
                })

    df_sku = pl.DataFrame(sku_rows)

    # --- Filters ---
    field_filter = mo.ui.dropdown(
        options=["All"] + FIELDS_TO_EXPORT,
        value="All",
        label="Field"
    )
    pattern_filter = mo.ui.dropdown(
        options=["All", "GS + LW agree, Shopify differs", "Shopify + LW agree, GS differs",
                 "GS + Shopify agree, LW differs", "All three differ"],
        value="All",
        label="Pattern"
    )
    search = mo.ui.text(placeholder="Search SKU...", label="SKU")
    return agg_table, df_sku, field_filter, pattern_filter, search, sku_rows


@app.cell
def _(
    agg_table,
    df_discrepancies,
    df_sku,
    field_filter,
    mo,
    pattern_filter,
    pl,
    search,
    sku_rows,
):
    # This cell re-runs automatically whenever filters change
    filtered = df_sku.clone()

    if field_filter.value != "All":
        filtered = filtered.filter(pl.col("Field") == field_filter.value)
    if pattern_filter.value != "All":
        filtered = filtered.filter(pl.col("Pattern") == pattern_filter.value)
    if search.value:
        filtered = filtered.filter(pl.col("SKU").str.contains(search.value))

    mo.vstack([
        mo.md("## Data quality dashboard"),
        mo.hstack([
            mo.stat(str(len(df_discrepancies)), label="Total SKUs"),
            mo.stat(str(len(sku_rows)), label="Total discrepancies"),
            mo.stat(str(len(df_sku["SKU"].unique())), label="SKUs affected"),
        ]),
        mo.md("### Aggregated view"),
        agg_table,
        mo.md("### SKU-level view"),
        mo.hstack([field_filter, pattern_filter, search]),
        mo.ui.table(filtered, label=f"{len(filtered)} rows"),
    ])
    return


if __name__ == "__main__":
    app.run()
