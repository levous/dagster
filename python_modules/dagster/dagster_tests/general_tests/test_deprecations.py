# This file strictly contains tests for deprecation warnings. It can serve as a central record of
# deprecations for the current version.

import re

import pytest
from dagster._core.definitions.input import InputDefinition
from dagster._core.definitions.output import OutputDefinition

# ########################
# ##### METADATA ARGUMENTS
# ########################


def test_arbitrary_metadata():
    with pytest.warns(DeprecationWarning, match=re.escape("arbitrary metadata values")):
        OutputDefinition(metadata={"foo": object()})

    with pytest.warns(DeprecationWarning, match=re.escape("arbitrary metadata values")):
        InputDefinition(name="foo", metadata={"foo": object()})
