from __future__ import annotations

import pytest
from snapflow.core.graph import Graph, graph_from_yaml
from snapflow.modules import core
from tests.utils import (
    make_test_env,
    snap_chain_t1_to_t2,
    snap_generic,
    snap_multiple_input,
    snap_self,
    snap_t1_source,
    snap_t1_to_t2,
)


def make_graph() -> Graph:
    env = make_test_env()
    env.add_module(core)
    g = Graph(env)
    g.create_node(key="node1", snap=snap_t1_source)
    g.node(key="node2", snap=snap_t1_source)
    g.node(key="node3", snap=snap_t1_to_t2, input="node1")
    g.node(key="node4", snap=snap_t1_to_t2, input="node2")
    g.node(key="node5", snap=snap_generic, input="node4")
    g.node(key="node6", snap=snap_self, input="node4")
    g.node(
        key="node7",
        snap=snap_multiple_input,
        inputs={"input": "node4", "other_t2": "node3"},
    )
    return g


def test_dupe_node():
    g = make_graph()
    with pytest.raises(KeyError):
        g.create_node(key="node1", snap=snap_t1_source)


def test_declared_graph():
    g = make_graph()
    n1 = g.get_node("node1")
    n2 = g.get_node("node2")
    n4 = g.get_node("node4")
    n5 = g.get_node("node5")
    assert len(list(g.all_nodes())) == 7
    assert g.get_all_upstream_dependencies_in_execution_order(n1) == [n1]
    assert g.get_all_upstream_dependencies_in_execution_order(n5) == [
        n2,
        n4,
        n5,
    ]


def test_make_graph():
    g = make_graph()
    nodes = {
        "node1",
        "node2",
        "node3",
        "node4",
        "node5",
        "node6",
        "node7",
    }
    assert set(n.key for n in g.all_nodes()) == nodes
    n3 = g.get_node("node3")
    n7 = g.get_node("node7")
    assert len(g.get_all_upstream_dependencies_in_execution_order(n3)) == 2
    assert len(g.get_all_upstream_dependencies_in_execution_order(n7)) == 5
    assert len(g.get_all_nodes_in_execution_order()) == len(nodes)
    execution_order = [n.key for n in g.get_all_nodes_in_execution_order()]
    expected_orderings = [
        ["node2", "node4", "node5",],
        ["node2", "node4", "node6",],
        ["node1", "node3", "node7",],
    ]
    # TODO: graph sort not stable!
    for ordering in expected_orderings:
        for i, n in enumerate(ordering[:-1]):
            assert execution_order.index(n) < execution_order.index(ordering[i + 1])


def test_graph_from_yaml():
    g = graph_from_yaml(
        """
    nodes:
      - key: stripe_charges
        snap: stripe.extract_charges
        params:
          api_key: "*****"
      - key: accumulated_stripe_charges
        snap: core.dataframe_accumulator
        inputs:
          - stripe_charges
      - key: stripe_customer_lifetime_sales
        snap: customer_lifetime_sales
        inputs:
          - accumulated_stripe_charges
    """
    )
    assert len(list(g.all_nodes())) == 3
    assert g.get_node("stripe_charges").params == {"api_key": "*****"}
    assert g.get_node("accumulated_stripe_charges").inputs == ["stripe_charges"]
    assert g.get_node("stripe_customer_lifetime_sales").inputs == [
        "accumulated_stripe_charges"
    ]
