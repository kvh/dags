from __future__ import annotations

import inspect
from dataclasses import asdict, dataclass, field
from functools import partial
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Type, Union, cast

from pandas import DataFrame
from snapflow.core.data_block import DataBlock, DataBlockMetadata
from snapflow.core.module import (
    DEFAULT_LOCAL_MODULE,
    DEFAULT_LOCAL_MODULE_NAME,
    SnapflowModule,
)
from snapflow.core.runtime import DatabaseRuntimeClass, PythonRuntimeClass, RuntimeClass
from snapflow.core.snap_interface import (
    DeclaredInput,
    DeclaredOutput,
    DeclaredSnapInterface,
    merge_declared_interface_with_signature_interface,
    snap_interface_from_callable,
)
from snapflow.schema.base import SchemaLike
from snapflow.storage.data_formats import DatabaseTableRef, Records

if TYPE_CHECKING:
    from snapflow.core.execution import SnapContext
    from snapflow import Environment


class SnapException(Exception):
    pass


class InputExhaustedException(SnapException):
    pass


SnapCallable = Callable[..., Any]

DataInterfaceType = Union[
    DataFrame, Records, DatabaseTableRef, DataBlockMetadata, DataBlock,
]  # TODO: also input...?   Isn't this duplicated with the Interface list AND with DataFormats?


def get_runtime_class(runtime: Optional[str]) -> Type[RuntimeClass]:
    if runtime is None:
        return PythonRuntimeClass
    if (
        "ql" in runtime.lower()
        or "database" in runtime.lower()
        or "postgre" in runtime.lower()
    ):
        return DatabaseRuntimeClass
    return PythonRuntimeClass


def make_snap_name(snap: Union[SnapCallable, _Snap, str]) -> str:
    # TODO: something more principled / explicit?
    if isinstance(snap, str):
        return snap
    if hasattr(snap, "name"):
        return snap.name  # type: ignore
    if hasattr(snap, "__name__"):
        return snap.__name__
    if hasattr(snap, "__class__"):
        return snap.__class__.__name__
    raise Exception(f"Cannot make name for snap-like {snap}")


@dataclass
class Parameter:
    name: str
    datatype: str
    required: bool = False
    default: Any = None
    help: str = ""


@dataclass  # (frozen=True)
class _Snap:
    # Underscored so the decorator API can use `Snap`. TODO: Is there a better way / name?
    name: str
    module_name: str
    snap_callable: Callable
    compatible_runtime_classes: List[Type[RuntimeClass]]
    params: List[Parameter] = field(default_factory=list)
    state_class: Optional[Type] = None
    declared_inputs: Optional[List[DeclaredInput]] = None
    declared_output: Optional[DeclaredOutput] = None
    ignore_signature: bool = (
        False  # Whether to ignore signature if there are any declared i/o
    )

    # TODO: runtime engine eg "mysql>=8.0", "python==3.7.4"  ???
    # TODO: runtime dependencies

    @property
    def key(self) -> str:
        k = self.name
        if self.module_name:
            k = self.module_name + "." + k
        return k

    def __call__(
        self, *args: SnapContext, **kwargs: DataInterfaceType
    ) -> Optional[DataInterfaceType]:
        return self.snap_callable(*args, **kwargs)

    def get_interface(self) -> DeclaredSnapInterface:
        """"""
        found_signature_interface = self._get_snap_interface()
        declared_interface = DeclaredSnapInterface(
            inputs=self.declared_inputs or [], output=self.declared_output
        )
        return merge_declared_interface_with_signature_interface(
            declared_interface,
            found_signature_interface,
            ignore_signature=self.ignore_signature,
        )

    def _get_snap_interface(self) -> DeclaredSnapInterface:
        if hasattr(self.snap_callable, "get_interface"):
            return self.snap_callable.get_interface()  # type: ignore
        return snap_interface_from_callable(self.snap_callable)

    def source_code_language(self) -> str:
        from snapflow.core.sql.snap import SqlSnapWrapper

        if isinstance(self.snap_callable, SqlSnapWrapper):
            return "sql"
        return "python"

    def get_source_code(self) -> Optional[str]:
        from snapflow.core.sql.snap import SqlSnapWrapper

        # TODO: more principled approach (can define a "get_source_code" otherwise we inspect?)
        if isinstance(self.snap_callable, SqlSnapWrapper):
            return self.snap_callable.sql
        return inspect.getsource(self.snap_callable)


SnapLike = Union[SnapCallable, _Snap]


def snap_factory(
    snap_like: Union[SnapCallable, _Snap],
    name: str = None,
    module: Optional[Union[SnapflowModule, str]] = None,
    compatible_runtimes: str = None,
    **kwargs: Any,
) -> _Snap:
    if name is None:
        assert snap_like is not None
        name = make_snap_name(snap_like)
    runtime_class = get_runtime_class(compatible_runtimes)
    if module is None:
        module = DEFAULT_LOCAL_MODULE
    if isinstance(module, SnapflowModule):
        module_name = module.name
    else:
        module_name = module
    if isinstance(snap_like, _Snap):
        # TODO: this is dicey, merging an existing snap ... which values take precedence?
        # old_attrs = asdict(snap_like)
        module_name = module_name or snap_like.module_name
        compatible_runtime_classes = (
            [runtime_class] if runtime_class else snap_like.compatible_runtime_classes
        )
        snap = _Snap(
            name=name,
            module_name=module_name,
            snap_callable=snap_like.snap_callable,
            compatible_runtime_classes=compatible_runtime_classes,
            params=kwargs.get("params") or snap_like.params,
            state_class=kwargs.get("state_class") or snap_like.state_class,
            declared_inputs=kwargs.get("declared_inputs") or snap_like.declared_inputs,
            declared_output=kwargs.get("declared_output") or snap_like.declared_output,
            ignore_signature=kwargs.get("ignore_signature")
            or snap_like.ignore_signature,
        )
    else:
        snap = _Snap(
            name=name,
            module_name=module_name,
            snap_callable=snap_like,
            compatible_runtime_classes=[runtime_class],
            **kwargs,
        )
    if module_name == DEFAULT_LOCAL_MODULE_NAME:
        # Add to default module
        DEFAULT_LOCAL_MODULE.add_snap(snap)
    return snap


def snap_decorator(
    snap_or_name: Union[str, SnapCallable, _Snap] = None,
    name: str = None,
    module: Optional[Union[SnapflowModule, str]] = None,
    compatible_runtimes: str = None,
    params: List[Parameter] = None,
    state_class: Optional[Type] = None,
    **kwargs,
) -> Union[Callable, _Snap]:
    if isinstance(snap_or_name, str) or snap_or_name is None:
        return partial(
            snap_decorator,
            module=module,
            name=name or snap_or_name,
            compatible_runtimes=compatible_runtimes,
            params=params,
            state_class=state_class,
            **kwargs,
        )
    return snap_factory(
        snap_or_name,
        name=name,
        module=module,
        compatible_runtimes=compatible_runtimes,
        params=params,
        state_class=state_class,
        **kwargs,
    )


def add_declared_input_decorator(
    name: str,
    schema: Optional[SchemaLike] = None,
    reference: bool = False,
    required: bool = True,
    from_self: bool = False,  # TODO: name
    stream: bool = False,
):
    inpt = DeclaredInput(
        name=name,
        schema_like=schema or "Any",
        reference=reference,
        _required=required,
        from_self=from_self,
        stream=stream,
    )

    def dec(snap_like: Union[SnapCallable, _Snap]) -> _Snap:
        if not isinstance(snap_like, _Snap):
            snap_like = snap_factory(snap_like)
        snap: _Snap = snap_like
        if snap.declared_inputs is None:
            snap.declared_inputs = [inpt]
        else:
            snap.declared_inputs.append(inpt)
        return snap

    return dec


def add_declared_output_decorator(
    schema: Optional[SchemaLike] = None,
    optional: bool = False,
    name: Optional[str] = None,
    stream: bool = False,
    default: bool = True,
):
    output = DeclaredOutput(
        name=name,
        schema_like=schema or "Any",
        optional=optional,
        stream=stream,
        default=default,
    )

    def dec(snap_like: Union[SnapCallable, _Snap]) -> _Snap:
        if not isinstance(snap_like, _Snap):
            snap_like = snap_factory(snap_like)
        snap: _Snap = snap_like
        snap.declared_output = output
        return snap

    return dec


def add_param_decorator(
    name: str,
    datatype: str,
    required: bool = False,
    default: Any = None,
    help: str = "",
):
    p = Parameter(
        name=name, datatype=datatype, required=required, default=default, help=help,
    )

    def dec(snap_like: Union[SnapCallable, _Snap]) -> _Snap:
        if not isinstance(snap_like, _Snap):
            snap_like = snap_factory(snap_like)
        snap: _Snap = snap_like
        snap.params.append(p)
        return snap

    return dec


def make_snap(snap_like: SnapLike, **kwargs) -> _Snap:
    if isinstance(snap_like, _Snap):
        return snap_like
    return snap_factory(snap_like, **kwargs)


def ensure_snap(env: Environment, snap_like: Union[SnapLike, str]) -> _Snap:
    if isinstance(snap_like, _Snap):
        return snap_like
    if isinstance(snap_like, str):
        return env.get_snap(snap_like)
    return make_snap(snap_like)


# Decorator API
Input = add_declared_input_decorator
Output = add_declared_output_decorator
Param = add_param_decorator
Snap = snap_decorator
