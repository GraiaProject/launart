from __future__ import annotations

import ast
import inspect
from collections.abc import Awaitable
from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from launart.manager import Launart
    from launart.service import Service


def patch_launch(serv: Service) -> Callable[[Launart], Awaitable[None]]:
    if serv.stages == {"preparing", "blocking", "cleanup"}:
        return serv.launch
    elif not serv.stages:

        async def _launch(manager: Launart):
            await serv.launch(manager)
            async with serv.stage("preparing"):
                pass
            async with serv.stage("blocking"):
                pass
            async with serv.stage("cleanup"):
                pass

        return _launch
    elif serv.stages == {"preparing", "blocking"}:

        async def _launch(manager: Launart):
            await serv.launch(manager)
            async with serv.stage("cleanup"):
                pass

        return _launch
    elif serv.stages == {"blocking", "cleanup"}:

        async def _launch(manager: Launart):
            async with serv.stage("preparing"):
                pass
            await serv.launch(manager)

        return _launch
    elif serv.stages == {"preparing"}:

        async def _launch(manager: Launart):
            await serv.launch(manager)
            async with serv.stage("blocking"):
                pass
            async with serv.stage("cleanup"):
                pass

        return _launch
    elif serv.stages == {"blocking"}:

        async def _launch(manager: Launart):
            async with serv.stage("preparing"):
                pass
            await serv.launch(manager)
            async with serv.stage("cleanup"):
                pass

        return _launch
    elif serv.stages == {"cleanup"}:

        async def _launch(manager: Launart):
            async with serv.stage("preparing"):
                pass
            async with serv.stage("blocking"):
                pass
            await serv.launch(manager)

        return _launch
    else:
        nodes = ast.parse(inspect.getsource(serv.__class__))
        for node in ast.walk(nodes):
            if isinstance(node, ast.AsyncFunctionDef) and node.name == "launch":
                break
        else:
            raise ValueError("this component has no launch method.")
        for index, _node in enumerate(node.body):
            if isinstance(_node, ast.AsyncWith):
                expr = _node.items[0].context_expr
                if (
                    isinstance(expr, ast.Call)
                    and isinstance(expr.func, ast.Attribute)
                    and isinstance(expr.func.value, ast.Name)
                    and expr.func.value.id == "self"
                    and expr.func.attr == "stage"
                ):
                    break
        else:
            raise ValueError("this component has no stage method.")

        new = ast.parse("async with self.stage('blocking'):\n    pass").body[0]
        new.lineno = _node.lineno + 1
        new.col_offset = node.col_offset + 4

        for other in node.body[index:]:
            for n in ast.walk(other):
                if hasattr(n, "lineno"):
                    n.lineno += 2  # type: ignore
                if hasattr(n, "end_lineno"):
                    n.end_lineno += 2  # type: ignore
        node.body.insert(index + 1, new)
        text = ast.unparse(node)
        lcs = {}
        exec(text, serv.launch.__globals__, lcs)  # noqa
        return lcs["launch"].__get__(serv, serv.__class__)
