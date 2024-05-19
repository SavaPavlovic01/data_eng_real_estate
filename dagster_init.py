from scraper import scrape_and_save, save_local, scrape_n_pages
from spark_test import merge_data
import json
from dagster import (
    asset,
    Definitions,
    define_asset_job,
    AssetSelection,
    ScheduleDefinition,
    load_assets_from_current_module,
    AssetExecutionContext,
    MaterializeResult,
    AssetIn
)

@asset
def scrape_asset(context:AssetExecutionContext):
    """
        Scrapes data from nekretnite.rs,
        saves it on S3 and passes the file name to the next asset
        Runs once per day
    """
    return save_local(json.dumps(scrape_n_pages(50)), 'raw_data')
    

@asset(ins={'scrape_asset':AssetIn(key = 'scrape_asset')})
def merge_asset(context:AssetExecutionContext, scrape_asset):
    """
        Reads data from S3,
        using Spark it adds it to a delta lake table on S3
    """
    # 's3a://warehouse/table' w
    merge_data(scrape_asset, 's3a://warehouse/table')

all_assets = load_assets_from_current_module()

scraper_job = define_asset_job('my_job', selection=AssetSelection.all())

scraper_schedule = ScheduleDefinition(
    job = scraper_job,
    cron_schedule = '0 0 * * *'
)

defs = Definitions(
    assets=all_assets,
    schedules=[scraper_schedule]
)