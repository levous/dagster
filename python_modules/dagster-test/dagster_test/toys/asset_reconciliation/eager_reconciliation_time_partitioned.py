from dagster import (
    AssetSelection,
    DailyPartitionsDefinition,
    Definitions,
    asset,
    build_asset_reconciliation_sensor,
    load_assets_from_current_module,
)

partitions_def = DailyPartitionsDefinition()


@asset
def eager_reconciliation_upstream():
    ...


@asset
def eager_reconciliation_downstream(eager_reconciliation_upstream):
    ...


defs = Definitions(
    assets=load_assets_from_current_module(group_name="eager_reconciliation"),
    sensors=build_asset_reconciliation_sensor(AssetSelection.groups("eager_reconciliation")),
)
