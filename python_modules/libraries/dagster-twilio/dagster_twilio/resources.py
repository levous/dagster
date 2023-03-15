from dagster import ConfigurableResource, resource
from dagster._config.structured_config import infer_schema_from_config_class
from dagster._core.execution.context.init import InitResourceContext
from pydantic import Field as PyField
from twilio.rest import Client


class TwilioResource(ConfigurableResource[Client]):
    """This resource is for connecting to Twilio."""

    account_sid: str = PyField(..., description="Twilio Account SID")
    auth_token: str = PyField(..., description="Twilio Auth Token")

    def create_resource(self, _context) -> Client:
        return Client(self.account_sid, self.auth_token)


@resource(
    config_schema=infer_schema_from_config_class(TwilioResource),
    description="This resource is for connecting to Twilio",
)
def twilio_resource(context: InitResourceContext) -> Client:
    return TwilioResource(**context.resource_config).resource_fn(context=context)  # type: ignore
