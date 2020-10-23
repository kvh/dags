from __future__ import annotations

from typing import Callable

import pytest
from pandas import DataFrame

from dags.core.data_block import DataBlock
from dags.core.graph import Graph
from dags.core.node import Node, create_node
from dags.core.pipe import Pipe, PipeInterface, PipeLike, pipe
from dags.core.pipe_interface import PipeAnnotation
from dags.core.runnable import PipeContext
from dags.core.runtime import RuntimeClass
from dags.core.sql.pipe import sql_pipe
from dags.modules import core
from dags.utils.typing import T, U
from tests.utils import (
    TestType1,
    make_test_env,
    pipe_chain_t1_to_t2,
    pipe_generic,
    pipe_self,
    pipe_t1_sink,
    pipe_t1_source,
    pipe_t1_to_t2,
)


@pytest.mark.parametrize(
    "annotation,expected",
    [
        (
            "DataBlock[Type]",
            PipeAnnotation(
                data_format_class="DataBlock",
                otype_like="Type",
                # is_iterable=False,
                is_generic=False,
                is_optional=False,
                is_variadic=False,
                original_annotation="DataBlock[Type]",
            ),
        ),
        (
            "DataSet[Type]",
            PipeAnnotation(
                data_format_class="DataSet",
                otype_like="Type",
                # is_iterable=False,
                is_generic=False,
                is_optional=False,
                is_variadic=False,
                original_annotation="DataSet[Type]",
            ),
        ),
        (
            "DataBlock[T]",
            PipeAnnotation(
                data_format_class="DataBlock",
                otype_like="T",
                # is_iterable=False,
                is_generic=True,
                is_optional=False,
                is_variadic=False,
                original_annotation="DataBlock[T]",
            ),
        ),
    ],
)
def test_typed_annotation(annotation: str, expected: PipeAnnotation):
    tda = PipeAnnotation.from_type_annotation(annotation)
    assert tda == expected


def pipe_notworking(_1: int, _2: str, input: DataBlock[TestType1]):
    # Bad args
    pass


def df4(input: DataBlock[T], dr2: DataBlock[U], dr3: DataBlock[U],) -> DataFrame[T]:
    pass


@pytest.mark.parametrize(
    "pipe,expected",
    [
        (
            pipe_t1_sink,
            PipeInterface(
                inputs=[
                    PipeAnnotation(
                        data_format_class="DataBlock",
                        otype_like="TestType1",
                        name="input",
                        # is_iterable=False,
                        is_generic=False,
                        is_optional=False,
                        is_variadic=False,
                        original_annotation="DataBlock[TestType1]",
                    )
                ],
                output=None,
                requires_pipe_context=True,
            ),
        ),
        (
            pipe_t1_to_t2,
            PipeInterface(
                inputs=[
                    PipeAnnotation(
                        data_format_class="DataBlock",
                        otype_like="TestType1",
                        name="input",
                        # is_iterable=False,
                        is_generic=False,
                        is_optional=False,
                        is_variadic=False,
                        original_annotation="DataBlock[TestType1]",
                    )
                ],
                output=PipeAnnotation(
                    data_format_class="DataFrame",
                    otype_like="TestType2",
                    # is_iterable=False,
                    is_generic=False,
                    is_optional=False,
                    is_variadic=False,
                    original_annotation="DataFrame[TestType2]",
                ),
                requires_pipe_context=False,
            ),
        ),
        (
            pipe_generic,
            PipeInterface(
                inputs=[
                    PipeAnnotation(
                        data_format_class="DataBlock",
                        otype_like="T",
                        name="input",
                        # is_iterable=False,
                        is_generic=True,
                        is_optional=False,
                        is_variadic=False,
                        original_annotation="DataBlock[T]",
                    )
                ],
                output=PipeAnnotation(
                    data_format_class="DataFrame",
                    otype_like="T",
                    # is_iterable=False,
                    is_generic=True,
                    is_optional=False,
                    is_variadic=False,
                    original_annotation="DataFrame[T]",
                ),
                requires_pipe_context=False,
            ),
        ),
        (
            pipe_self,
            PipeInterface(
                inputs=[
                    PipeAnnotation(
                        data_format_class="DataBlock",
                        otype_like="T",
                        name="input",
                        # is_iterable=False,
                        is_generic=True,
                        is_optional=False,
                        is_variadic=False,
                        is_self_ref=False,
                        original_annotation="DataBlock[T]",
                    ),
                    PipeAnnotation(
                        data_format_class="DataBlock",
                        otype_like="T",
                        name="this",
                        # is_iterable=False,
                        is_generic=True,
                        is_optional=True,
                        is_variadic=False,
                        is_self_ref=True,
                        original_annotation="DataBlock[T]",
                    ),
                ],
                output=PipeAnnotation(
                    data_format_class="DataFrame",
                    otype_like="T",
                    # is_iterable=False,
                    is_generic=True,
                    is_optional=False,
                    is_variadic=False,
                    original_annotation="DataFrame[T]",
                ),
                requires_pipe_context=False,
            ),
        ),
        (
            pipe_chain_t1_to_t2,
            PipeInterface(
                inputs=[
                    PipeAnnotation(
                        data_format_class="DataBlock",
                        otype_like="TestType1",
                        name="input",
                        # is_iterable=False,
                        is_generic=False,
                        is_optional=False,
                        is_variadic=False,
                        original_annotation="DataBlock[TestType1]",
                    )
                ],
                output=PipeAnnotation(
                    data_format_class="DataFrame",
                    otype_like="T",
                    # is_iterable=False,
                    is_generic=True,
                    is_optional=False,
                    is_variadic=False,
                    original_annotation="DataFrame[T]",
                ),
                requires_pipe_context=False,
            ),
        ),
    ],
)
def test_pipe_interface(pipe: PipeLike, expected: PipeInterface):
    env = make_test_env()
    g = Graph(env)
    if isinstance(pipe, Pipe):
        val = pipe.get_interface(env)
    elif isinstance(pipe, Callable):
        val = PipeInterface.from_pipe_definition(pipe)
    else:
        raise
    assert val == expected
    node = create_node(g, "_test", pipe, inputs="mock")
    assert node.get_interface() == expected


# env = make_test_env()
# upstream = env.add_node("_test_df1", pipe_t1_source)
#
#
# @pytest.mark.parametrize(
#     "pipe,expected",
#     [
#         (
#             pipe_t1_sink,
#             PipeInterface(
#                 inputs=[
#                     PipeAnnotation(
#                         data_format_class="DataBlock",
#                         otype_like="TestType1",
#                         resolved_otype=TestType1,
#                         connected_stream=upstream,
#                         name="input",
#                         is_iterable=False,
#                         is_optional=False,
#                         original_annotation="DataBlock[TestType1]",
#                     )
#                 ],
#                 output=None,
#                 requires_pipe_context=True,
#                 is_connected=True,
#                 is_resolved=True,
#             ),
#         ),
#         (
#             pipe_generic,
#             PipeInterface(
#                 inputs=[
#                     PipeAnnotation(
#                         data_format_class="DataBlock",
#                         otype_like="T",
#                         resolved_otype=TestType1,
#                         connected_stream=upstream,
#                         name="input",
#                         is_generic=True,
#                         is_iterable=False,
#                         is_optional=False,
#                         original_annotation="DataBlock[T]",
#                     )
#                 ],
#                 output=PipeAnnotation(
#                     data_format_class="DataFrame",
#                     otype_like="T",
#                     resolved_otype=TestType1,
#                     is_generic=True,
#                     is_iterable=False,
#                     is_optional=False,
#                     original_annotation="DataFrame[T]",
#                 ),
#                 requires_pipe_context=False,
#                 is_connected=True,
#                 is_resolved=True,
#             ),
#         ),
#     ],
# )
# def test_concrete_pipe_interface(
#     pipe: Callable, expected: PipeInterface
# ):
#     dfi = PipeInterface.from_pipe_definition(pipe)
#     dfi.connect_upstream(upstream)
#     dfi.resolve_otypes(env)
#     assert dfi == expected


def test_inputs():
    env = make_test_env()
    g = Graph(env)
    n1 = g.add_node("node1", pipe_t1_source)
    n2 = g.add_node("node2", pipe_t1_to_t2, inputs={"input": "node1"})
    dfi = n2.get_interface()
    assert dfi is not None
    n3 = g.add_node("node3", pipe_chain_t1_to_t2, inputs="node1")
    dfi = n3.get_interface()
    assert dfi is not None


# def test_stream_input():
#     env = make_test_env()
#     env.add_module(core)
#     n1 = env.add_node("node1", pipe_t1_source)
#     n2 = env.add_node("node2", pipe_t1_source)
#     n3 = env.add_node("node3", pipe_chain_t1_to_t2, inputs="node1")
#     ds1 = env.add_node(
#         "ds1",
#         accumulate_as_dataset,
#         config=dict(dataset_name="type1"),
#         upstream=DataBlockStream(otype="TestType1"),
#     )
#     dfi = ds1.get_interface()


def test_python_pipe():
    env = make_test_env()
    g = Graph(env)
    df = pipe(pipe_t1_sink)
    assert (
        df.key == pipe_t1_sink.__name__
    )  # TODO: do we really want this implicit name? As long as we error on duplicate should be ok

    k = "name1"
    df = pipe(pipe_t1_sink, key=k)
    assert df.key == k

    dfi = df.get_interface(env)
    assert dfi is not None


@pipe("k1", compatible_runtimes="python")
def df1():
    pass


@pipe("k1", compatible_runtimes="mysql")
def df2():
    pass


# def test_pipe_registry():
#     r = PipeRegistry()
#     dfs = Pipe(name="k1", module_key=DEFAULT_module_key, version=None)
#     dfs.add_definition(df1)
#     r.process_and_register_all([pipe_t1_sink, pipe_chain_t1_to_t2, dfs, df2])
#     assert r.get("k1") is dfs
#     assert r.get("k1").runtime_pipes[RuntimeClass.PYTHON] is df1
#     assert r.get("k1").runtime_pipes[RuntimeClass.DATABASE] is df2


def test_node_no_inputs():
    env = make_test_env()
    g = Graph(env)
    df = pipe(pipe_t1_source)
    node1 = create_node(g, "node1", df)
    assert {node1: node1}[node1] is node1  # Test hash
    dfi = node1.get_interface()
    assert dfi.inputs == []
    assert dfi.output is not None
    assert node1.get_declared_inputs() == {}
    assert not node1.is_composite()


def test_node_inputs():
    env = make_test_env()
    g = Graph(env)
    df = pipe(pipe_t1_source)
    node = create_node(g, "node", df)
    df = pipe(pipe_t1_sink)
    with pytest.raises(Exception):
        # Bad input
        create_node(g, "node_fail", df, input="Turname")  # type: ignore
    node1 = create_node(g, "node1", df, inputs=node)
    dfi = node1.get_interface()
    dfi = node1.get_interface()
    assert len(dfi.inputs) == 1
    assert dfi.output is None
    assert list(node1.get_declared_inputs().keys()) == ["input"]
    # assert node1.get_input("input").get_upstream(env)[0] is node


def test_node_config():
    env = make_test_env()
    g = Graph(env)
    config_vals = []

    def pipe_ctx(ctx: PipeContext):
        config_vals.append(ctx.get_config_value("test"))

    n = g.add_node("ctx", pipe_ctx, config={"test": 1, "extra_arg": 2})
    with env.execution(g) as exe:
        exe.run(n)
    assert config_vals == [1]


# def test_node_chain():
#     env = make_test_env()
#     df = pipe(pipe_t1_source)
#     node = Node(env, "node", df)
#     node1 = PipeNodeChain(env, "node1", pipe_chain_t1_to_t2, upstream=node)
#     dfi = node1.get_interface()
#     assert len(dfi.inputs) == 1
#     assert dfi.output is not None
#     assert list(node1.get_inputs().keys()) == ["input"]
#     assert node1.get_input("input").get_upstream(env)[0] is node
#     # Output NODE
#     output_node = node1.get_output_node()
#     assert output_node is not node1
#     assert node1.name in output_node.key
#     out_dfi = output_node.get_interface()
#     assert len(out_dfi.inputs) == 1
#     assert out_dfi.output is not None
#     # Children
#     assert len(node1.get_nodes()) == 2


# def test_graph_resolution():
#     env = make_test_env()
#     n1 = env.add_node("node1", pipe_t1_source)
#     n2 = env.add_node("node2", pipe_t1_source)
#     n3 = env.add_node("node3", pipe_chain_t1_to_t2, upstream="node1")
#     n4 = env.add_node("node4", pipe_t1_to_t2, upstream="node2")
#     n5 = env.add_node("node5", pipe_generic, upstream="node4")
#     n6 = env.add_node("node6", pipe_self, upstream="node4")
#     fgr = PipeGraphResolver(env)
#     # Resolve types
#     assert fgr.resolve_output_type(n4) is TestType2
#     assert fgr.resolve_output_type(n5) is TestType2
#     assert fgr.resolve_output_type(n6) is TestType2
#     fgr.resolve_output_types()
#     last = n3.get_nodes()[-1]
#     assert fgr._resolved_output_types[last] is TestType2
#     # Resolve deps
#     assert fgr._resolve_node_dependencies(n4)[0].parent_nodes == [n2]
#     assert fgr._resolve_node_dependencies(n5)[0].parent_nodes == [n4]
#     fgr.resolve_dependencies()
#     # Otype resolution
#     n7 = env.add_node("node7", pipe_self, upstream=DataBlockStream(otype="TestType2"))
#     n8 = env.add_node("node8", pipe_self, upstream=n7)
#     fgr = env.get_pipe_graph_resolver()
#     fgr.resolve()
#     parent_keys = set(
#         p.name for p in fgr.get_resolved_interface(n7).inputs[0].parent_nodes
#     )
#     assert parent_keys == {
#         "node3__pipe_t1_to_t2",
#         "node3__pipe_generic",
#         "node4",
#         "node5",
#         "node6",
#     }


def test_any_otype_interface():
    env = make_test_env()
    env.add_module(core)

    def pipe_any(input: DataBlock) -> DataFrame:
        pass

    df = pipe(pipe_any)
    dfi = df.get_interface(env)
    assert dfi.inputs[0].otype_like == "Any"
    assert dfi.output.otype_like == "Any"