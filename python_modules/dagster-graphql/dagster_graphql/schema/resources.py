from typing import List, cast

import dagster._check as check
import graphene
from dagster._config.structured_config.resource_verification import VerificationStatus
from dagster._core.definitions.metadata import TextMetadataValue
from dagster._core.definitions.selector import ResourceSelector
from dagster._core.events import DagsterEventType
from dagster._core.execution.plan.outputs import StepOutputData
from dagster._core.host_representation.external import ExternalResource
from dagster._core.host_representation.external_data import (
    ExternalResourceConfigEnvVar,
    ExternalResourceValue,
    NestedResourceType,
)
from dagster._core.instance import DagsterInstance
from dagster._core.storage.pipeline_run import RunsFilter

from dagster_graphql.schema.asset_key import GrapheneAssetKey
from dagster_graphql.schema.errors import (
    GraphenePythonError,
    GrapheneRepositoryNotFoundError,
    GrapheneResourceNotFoundError,
)
from dagster_graphql.schema.util import non_null_list

from .config_types import GrapheneConfigTypeField


class GrapheneConfiguredValueType(graphene.Enum):
    VALUE = "VALUE"
    ENV_VAR = "ENV_VAR"

    class Meta:
        name = "ConfiguredValueType"


class GrapheneConfiguredValue(graphene.ObjectType):
    key = graphene.NonNull(graphene.String)
    value = graphene.NonNull(graphene.String)
    type = graphene.NonNull(GrapheneConfiguredValueType)

    class Meta:
        name = "ConfiguredValue"

    def __init__(self, key: str, external_resource_value: ExternalResourceValue):
        super().__init__()

        self.key = key
        if isinstance(external_resource_value, ExternalResourceConfigEnvVar):
            self.type = GrapheneConfiguredValueType.ENV_VAR
            self.value = external_resource_value.name
        else:
            self.type = GrapheneConfiguredValueType.VALUE
            self.value = external_resource_value


GrapheneNestedResourceType = graphene.Enum.from_enum(NestedResourceType)


class GrapheneNestedResourceEntry(graphene.ObjectType):
    name = graphene.NonNull(graphene.String)
    type = graphene.NonNull(GrapheneNestedResourceType)
    resource = graphene.Field(lambda: GrapheneResourceDetails)

    class Meta:
        name = "NestedResourceEntry"


class GrapheneJobWithOps(graphene.ObjectType):
    jobName = graphene.NonNull(graphene.String)
    ops = graphene.Field(non_null_list(graphene.String))

    class Meta:
        name = "JobWithOps"


GrapheneResourceVerificationStatus = graphene.Enum.from_enum(VerificationStatus)


class GrapheneResourceVerificationResult(graphene.ObjectType):
    status = graphene.NonNull(GrapheneResourceVerificationStatus)
    message = graphene.NonNull(graphene.String)

    class Meta:
        name = "ResourceVerificationResult"


class GrapheneResourceDetails(graphene.ObjectType):
    name = graphene.NonNull(graphene.String)
    description = graphene.String()
    configFields = graphene.Field(
        non_null_list(GrapheneConfigTypeField),
        description="Snapshots of all the fields for the given resource",
    )
    configuredValues = graphene.Field(
        non_null_list(GrapheneConfiguredValue),
        description=(
            "List of K/V pairs of user-configured values for each of the top-level fields on the"
            " resource"
        ),
    )
    isTopLevel = graphene.NonNull(graphene.Boolean)
    nestedResources = graphene.Field(
        non_null_list(GrapheneNestedResourceEntry),
        description="List of nested resources for the given resource",
    )
    parentResources = graphene.Field(
        non_null_list(GrapheneNestedResourceEntry),
        description="List of parent resources for the given resource",
    )
    resourceType = graphene.NonNull(graphene.String)
    assetKeysUsing = graphene.Field(non_null_list(GrapheneAssetKey))
    jobsOpsUsing = graphene.Field(non_null_list(GrapheneJobWithOps))
    supportsVerification = graphene.NonNull(graphene.Boolean)
    verificationResult = graphene.Field(GrapheneResourceVerificationResult)

    class Meta:
        name = "ResourceDetails"

    def __init__(
        self, location_name: str, repository_name: str, external_resource: ExternalResource
    ):
        super().__init__()

        self._location_name = check.str_param(location_name, "location_name")
        self._repository_name = check.str_param(repository_name, "repository_name")

        self._external_resource = check.inst_param(
            external_resource, "external_resource", ExternalResource
        )
        self.name = external_resource.name
        self.description = external_resource.description
        self._config_field_snaps = external_resource.config_field_snaps
        self._configured_values = external_resource.configured_values

        self._config_schema_snap = external_resource.config_schema_snap
        self.isTopLevel = external_resource.is_top_level
        self._nested_resources = external_resource.nested_resources
        self._parent_resources = external_resource.parent_resources
        self.resourceType = external_resource.resource_type
        self._asset_keys_using = external_resource.asset_keys_using
        self._job_ops_using = external_resource.job_ops_using
        self.supportsVerification = "verification" in external_resource.capabilities

    def resolve_configFields(self, _graphene_info):
        return [
            GrapheneConfigTypeField(
                config_schema_snapshot=self._config_schema_snap,
                field_snap=field_snap,
            )
            for field_snap in self._config_field_snaps
        ]

    def resolve_configuredValues(self, _graphene_info):
        return [
            GrapheneConfiguredValue(key=key, external_resource_value=value)
            for key, value in self._configured_values.items()
        ]

    def resolve_nestedResources(self, graphene_info) -> List[GrapheneNestedResourceEntry]:
        from dagster_graphql.implementation.fetch_resources import get_resource_or_error

        return [
            GrapheneNestedResourceEntry(
                name=k,
                type=v.type,
                resource=get_resource_or_error(
                    graphene_info,
                    ResourceSelector(
                        location_name=self._location_name,
                        repository_name=self._repository_name,
                        resource_name=v.name,
                    ),
                )
                if v.type == NestedResourceType.TOP_LEVEL
                else None,
            )
            for k, v in self._nested_resources.items()
        ]

    def resolve_parentResources(self, graphene_info) -> List[GrapheneNestedResourceEntry]:
        from dagster_graphql.implementation.fetch_resources import get_resource_or_error

        return [
            GrapheneNestedResourceEntry(
                name=attribute,
                type=NestedResourceType.TOP_LEVEL,
                resource=get_resource_or_error(
                    graphene_info,
                    ResourceSelector(
                        location_name=self._location_name,
                        repository_name=self._repository_name,
                        resource_name=name,
                    ),
                ),
            )
            for name, attribute in self._parent_resources.items()
        ]

    def resolve_assetKeysUsing(self, _graphene_info) -> List[GrapheneAssetKey]:
        return [GrapheneAssetKey(path=asset_key.path) for asset_key in self._asset_keys_using]

    def resolve_jobsOpsUsing(self, _graphene_info) -> List[GrapheneJobWithOps]:
        return [
            GrapheneJobWithOps(jobName=job_name, ops=ops)
            for job_name, ops in self._job_ops_using.items()
        ]

    def resolve_verificationResult(self, graphene_info) -> GrapheneResourceVerificationResult:
        instance: DagsterInstance = graphene_info.context.instance
        most_recent_runs = list(
            instance.run_storage.get_runs(RunsFilter(job_name="__CONFIG_CHECK_github"), limit=1)
        )
        if len(most_recent_runs) == 0:
            return GrapheneResourceVerificationStatus(VerificationStatus.FAILURE, "none yet")
        run_id = most_recent_runs[0].run_id

        records = instance.all_logs(run_id=run_id, of_type=DagsterEventType.STEP_OUTPUT)
        # records = instance.event_log_storage.get_event_records(
        #     event_records_filter=EventRecordsFilter(
        #         DagsterEventType.HANDLED_OUTPUT, tags={"pipeline_name": "__CONFIG_CHECK_github"}
        #     ),
        #     limit=5,
        # )
        # for record in records:
        #     print(record.dagster_event_type)
        status = None
        message = None
        for record in records:
            step_output_data = (
                cast(StepOutputData, record.dagster_event.event_specific_data)
                if record.dagster_event
                else None
            )
            if step_output_data:
                entry_dict = {
                    entry.label: entry.value for entry in step_output_data.metadata_entries
                }
                status_entry = entry_dict.get("status")
                if isinstance(status_entry, TextMetadataValue):
                    status = status_entry.text
                message_entry = entry_dict.get("message")
                if isinstance(message_entry, TextMetadataValue):
                    message = message_entry.text
        return GrapheneResourceVerificationResult(VerificationStatus(status), message or "")


class GrapheneResourceDetailsOrError(graphene.Union):
    class Meta:
        types = (GrapheneResourceDetails, GrapheneResourceNotFoundError, GraphenePythonError)
        name = "ResourceDetailsOrError"


class GrapheneResourceDetailsList(graphene.ObjectType):
    results = non_null_list(GrapheneResourceDetails)

    class Meta:
        name = "ResourceDetailsList"


class GrapheneResourceDetailsListOrError(graphene.Union):
    class Meta:
        types = (GrapheneResourceDetailsList, GrapheneRepositoryNotFoundError, GraphenePythonError)
        name = "ResourcesOrError"
