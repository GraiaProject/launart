from __future__ import annotations

from typing import TYPE_CHECKING, Iterable

if TYPE_CHECKING:
    from .service import Service


class RequirementResolveFailed(Exception):
    pass


class DependencyBrokenError(Exception):
    pass


def _build_dependencies_map(services: Iterable[Service]) -> dict[str, set[str]]:
    dependencies_map: dict[str, set[str]] = {}

    for service in services:
        dependencies_map[service.id] = set(service.dependencies) | set(service.after)

        for before in service.before:
            dependencies_map.setdefault(before, set()).add(service.id)

    return dependencies_map


def resolve_dependencies(
    services: Iterable[Service],
    exclude: Iterable[Service] = (),
    *,
    reverse: bool = False,
) -> list[list[str]]:
    services = list(services)

    dependencies_map = _build_dependencies_map(services)

    unresolved = {s.id: s for s in services}
    resolved_id = {i.id for i in exclude}
    result: list[list[str]] = []

    while unresolved:
        layer_candidates = [service for service in unresolved.values() if resolved_id.issuperset(dependencies_map[service.id])]

        if not layer_candidates:
            raise TypeError("Failed to resolve requirements due to cyclic dependencies or unmet constraints.")

        # 根据是否有 before 约束进行分类
        befores = []
        no_befores = []

        for service in layer_candidates:
            if service.before:
                befores.append(service)
            else:
                no_befores.append(service)

        # 优先无 before 的服务，一旦无 before 的服务存在，就先放这一层
        current_layer = no_befores or befores

        # 从未解决中移除当前层的服务
        for cid in current_layer:
            del unresolved[cid]

        resolved_id.update(current_layer)
        result.append(current_layer)

    if reverse:
        result.reverse()

    return result


def validate_services_removal(existed: Iterable[Service], services_to_remove: Iterable[Service]):
    graph = {service.id: set() for service in existed}

    for service, deps in _build_dependencies_map(existed).items():
        for dep in deps:
            if dep in graph:
                graph[dep].add(service)

    to_remove = {service.id for service in services_to_remove}

    for node in to_remove:
        for dependent in graph.get(node, ()):
            if dependent not in to_remove:
                raise DependencyBrokenError(f"Cannot remove node '{node}' because node '{dependent}' depends on it.")
