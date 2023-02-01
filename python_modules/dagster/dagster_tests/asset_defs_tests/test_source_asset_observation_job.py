from typing import Optional

from dagster._core.definitions.data_version import (
    DataVersion,
    extract_data_version_from_entry,
)
from dagster._core.definitions.decorators.source_asset_decorator import observable_source_asset
from dagster._core.definitions.definitions_class import Definitions
from dagster._core.definitions.events import AssetKey
from dagster._core.definitions.resource_definition import ResourceDefinition
from dagster._core.definitions.unresolved_asset_job_definition import define_asset_job
from dagster._core.errors import DagsterInvalidDefinitionError
from dagster._core.instance import DagsterInstance


def _get_current_data_version(key: AssetKey, instance: DagsterInstance) -> Optional[DataVersion]:
    record = instance.get_latest_data_version_record(key)
    assert record is not None
    return extract_data_version_from_entry(record.event_log_entry)


def test_execute_source_asset_observation_job():
    executed = {}

    @observable_source_asset
    def foo(_context) -> DataVersion:
        executed["foo"] = True
        return DataVersion("alpha")

    @observable_source_asset
    def bar(context):
        executed["bar"] = True
        return DataVersion("beta")

    instance = DagsterInstance.ephemeral()

    result = (
        Definitions(
            assets=[foo, bar],
            jobs=[define_asset_job("source_asset_job", [foo, bar])],
        )
        .get_job_def("source_asset_job")
        .execute_in_process(instance=instance)
    )

    assert result.success
    assert executed["foo"]
    assert _get_current_data_version(AssetKey("foo"), instance) == DataVersion("alpha")
    assert executed["bar"]
    assert _get_current_data_version(AssetKey("bar"), instance) == DataVersion("beta")


@pytest.mark.parametrize(
    "is_valid,resource_defs",
    [(True, {"bar": ResourceDefinition.hardcoded_resource("bar")}), (False, {})],
)
def test_source_asset_observation_job_with_resource(is_valid, resource_defs):
    executed = {}

    @observable_source_asset(
        required_resource_keys={"bar"},
    )
    def foo(context) -> DataVersion:
        executed["foo"] = True
        return DataVersion(f"{context.resources.bar}")

    instance = DagsterInstance.ephemeral()

    if is_valid:
        print("RESOURCE DEFS", resource_defs)
        result = (
            Definitions(
                assets=[foo],
                jobs=[define_asset_job("source_asset_job", [foo])],
                resources=resource_defs,
            )
            .get_job_def("source_asset_job")
            .execute_in_process(instance=instance)
        )

        assert result.success
        assert executed["foo"]
        assert _get_current_data_version(AssetKey("foo"), instance) == DataVersion("bar")
    else:
        with pytest.raises(
            DagsterInvalidDefinitionError,
            match="resource with key 'bar' required by op 'foo' was not provided",
        ):
            result = (
                Definitions(
                    assets=[foo],
                    jobs=[define_asset_job("source_asset_job", [foo])],
                    resources=resource_defs,
                )
                .get_job_def("source_asset_job")
                .execute_in_process(instance=instance)
            )
