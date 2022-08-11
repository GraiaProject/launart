from typing import Any

import pytest

from launart.component import RequirementResolveFailed, resolve_requirements
from launart.service import ExportInterface
from launart.utilles import priority_strategy
from tests.fixture import component_standalone, interface


def test_resolve_success():
    dataset = [
        component_standalone("a", []),
        component_standalone("b", ["a"]),
        component_standalone("c", ["a", "b"]),
        component_standalone("d", ["a", "c"]),
        component_standalone("e", []),
        component_standalone("f", ["a", "c"]),
    ]
    expected = [{dataset[0], dataset[4]}, {dataset[1]}, {dataset[2]}, {dataset[3], dataset[5]}]
    assert resolve_requirements(dataset) == expected
    expected.reverse()
    assert resolve_requirements(dataset, reverse=True) == expected


def test_resolve_fail():
    dataset = [
        component_standalone("a", ["b"]),
        component_standalone("b", ["a"]),
    ]
    with pytest.raises(RequirementResolveFailed):
        resolve_requirements(dataset)


def test_priority_strategy():
    def spit(v: Any) -> Any:
        return v

    with pytest.raises(TypeError):
        priority_strategy([1, 2, 3], spit)
    with pytest.raises(TypeError):
        priority_strategy([(1, 2), 3], spit)

    i = [interface() for _ in range(10)]

    s1 = {i[0], i[1]}
    s2 = {i[1], i[2]}
    s3 = {i[2], i[3]}

    d1 = {i[0]: 1, i[1]: 2}
    d2 = {i[1]: 1, i[2]: 2}
    d3 = {i[2]: 1, i[3]: 2}

    assert priority_strategy([s1, s3], spit) == {i[0]: s1, i[1]: s1, i[2]: s3, i[3]: s3}

    assert priority_strategy([d1, d2, d3], spit) == {i[0]: d1, i[1]: d1, i[2]: d2, i[3]: d3}

    assert priority_strategy([d1, d3, d2], spit) == {i[0]: d1, i[1]: d1, i[2]: d2, i[3]: d3}

    assert priority_strategy([(d1, d2), d3], spit) == {i[0]: (d1, d2), i[1]: (d1, d2), i[2]: (d1, d2), i[3]: d3}

    with pytest.raises(ValueError):
        priority_strategy([s1, s2], spit)
    with pytest.raises(ValueError):
        priority_strategy([s1, d1], spit)