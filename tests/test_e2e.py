from __future__ import annotations

import sys
from datetime import datetime
from typing import Generator, Optional

import pandas as pd
import pytest
from loguru import logger
from pandas._testing import assert_almost_equal
from snapflow import DataBlock, Input, Output, Param, Snap, sql_snap
from snapflow.core.environment import Environment, produce
from snapflow.core.execution import SnapContext
from snapflow.core.graph import Graph
from snapflow.core.node import DataBlockLog, NodeState, SnapLog
from snapflow.modules import core
from snapflow.schema.base import create_quick_schema
from snapflow.storage.data_formats import Records, RecordsIterator
from snapflow.storage.db.utils import get_tmp_sqlite_db_url
from snapflow.storage.storage import new_local_python_storage

Customer = create_quick_schema(
    "Customer", [("name", "Unicode"), ("joined", "DateTime"), ("metadata", "JSON")]
)
Metric = create_quick_schema(
    "Metric", [("metric", "Unicode"), ("value", "Numeric(12,2)")]
)


@Snap
def shape_metrics(i1: DataBlock) -> Records[Metric]:
    df = i1.as_dataframe()
    return [
        {"metric": "row_count", "value": len(df)},
        {"metric": "col_count", "value": len(df.columns)},
    ]


@Snap
def aggregate_metrics(
    i1: DataBlock, this: Optional[DataBlock] = None
) -> Records[Metric]:
    if this is not None:
        metrics = this.as_records()
    else:
        metrics = [
            {"metric": "row_count", "value": 0},
            {"metric": "blocks", "value": 0},
        ]
    df = i1.as_dataframe()
    rows = len(df)
    for m in metrics:
        if m["metric"] == "row_count":
            m["value"] += rows
        if m["metric"] == "blocks":
            m["value"] += 1
    return metrics


FAIL_MSG = "Failure triggered"


@Snap
def customer_source(ctx: SnapContext) -> RecordsIterator[Customer]:
    N = ctx.get_param("total_records")
    fail = ctx.get_param("fail")
    n = ctx.get_state_value("records_extracted", 0)
    if n >= N:
        return
    for i in range(2):
        records = []
        for j in range(2):
            records.append(
                {
                    "name": f"name{n}",
                    "joined": datetime(2000, 1, n + 1),
                    "metadata": {"idx": n},
                }
            )
            n += 1
        yield records
        ctx.emit_state_value("records_extracted", n)
        if fail:
            # Fail AFTER yielding one record set
            raise Exception(FAIL_MSG)
        if n >= N:
            return


aggregate_metrics_sql = sql_snap(
    "aggregate_metrics_sql",
    sql="""
    select -- :Metric
        'row_count' as metric,
        count(*) as value
    from input
    """,
)


dataset_inputs_sql = sql_snap(
    "dataset_inputs_sql",
    sql="""
    select
        'input' as tble
        , count(*) as row_count
    from input
    union all
    select
        'metrics' as tble
        , count(*) as row_count
    from metrics
    """,
)


mixed_inputs_sql = sql_snap(
    "mixed_inputs_sql",
    sql="""
    select
        'input' as tble
        , count(*) as row_count
    from input -- :DataBlock
    union all
    select
        'metrics' as tble
        , count(*) as row_count
    from metrics
    """,
)


def get_env():
    env = Environment(metadata_storage=get_tmp_sqlite_db_url())
    env.add_module(core)
    env.add_schema(Customer)
    env.add_schema(Metric)
    return env


def test_simple_extract():
    dburl = get_tmp_sqlite_db_url()
    env = Environment(metadata_storage=dburl)
    g = Graph(env)
    env.add_module(core)
    df = pd.DataFrame({"a": range(10), "b": range(10)})
    g.create_node(key="n1", snap="extract_dataframe", params={"dataframe": df})
    output = env.produce("n1", g)
    assert_almost_equal(output.as_dataframe(), df)


def test_repeated_runs():
    env = get_env()
    g = Graph(env)
    s = env._local_python_storage
    # Initial graph
    N = 2 * 4
    g.create_node(key="source", snap=customer_source, params={"total_records": N})
    metrics = g.create_node(key="metrics", snap=shape_metrics, input="source")
    # Run first time
    output = env.produce("metrics", g, target_storage=s)
    assert output.nominal_schema_key.endswith("Metric")
    records = output.as_records()
    expected_records = [
        {"metric": "row_count", "value": 2},  # Just the latest block
        {"metric": "col_count", "value": 3},
    ]
    assert records == expected_records
    # Run again, should get next batch
    output = env.produce("metrics", g, target_storage=s)
    records = output.as_records()
    assert records == expected_records
    # Test latest_output
    output = env.get_latest_output(metrics)
    records = output.as_records()
    assert records == expected_records
    # Run again, should be exhausted
    output = env.produce("metrics", g, target_storage=s)
    assert output is None
    # Run again, should still be exhausted
    output = env.produce("metrics", g, target_storage=s)
    assert output is None

    # now add new node and process all at once
    g.create_node(
        key="new_accumulator", snap="core.dataframe_accumulator", input="source"
    )
    output = env.produce("new_accumulator", g, target_storage=s)
    records = output.as_records()
    assert len(records) == N
    output = env.produce("new_accumulator", g, target_storage=s)
    assert output is None


def test_alternate_apis():
    env = get_env()
    g = Graph(env)
    s = env._local_python_storage
    # Initial graph
    N = 2 * 4
    source = g.create_node(customer_source, params={"total_records": N})
    metrics = g.create_node(shape_metrics, input=source)
    # Run first time
    output = produce(metrics, graph=g, target_storage=s, env=env)
    assert output.nominal_schema_key.endswith("Metric")
    records = output.as_records()
    expected_records = [
        {"metric": "row_count", "value": 2},  # Just the last block
        {"metric": "col_count", "value": 3},
    ]
    assert records == expected_records


def test_snap_failure():
    env = get_env()
    g = Graph(env)
    s = env._local_python_storage
    # Initial graph
    N = 2 * 4
    cfg = {"total_records": N, "fail": True}
    source = g.create_node(customer_source, params=cfg)
    output = produce(source, graph=g, target_storage=s, env=env)
    assert output is not None
    records = output.as_records()
    assert len(records) == 2
    with env.session_scope() as sess:
        assert sess.query(SnapLog).count() == 1
        assert sess.query(DataBlockLog).count() == 1
        pl = sess.query(SnapLog).first()
        assert pl.node_key == source.key
        assert pl.graph_id == g.get_metadata_obj().hash
        assert pl.node_start_state == {}
        assert pl.node_end_state == {"records_extracted": 2}
        assert pl.snap_key == source.snap.key
        assert pl.snap_params == cfg
        assert pl.error is not None
        assert FAIL_MSG in pl.error["error"]
        ns = sess.query(NodeState).filter(NodeState.node_key == pl.node_key).first()
        assert ns.state == {"records_extracted": 2}

    source.params["fail"] = False
    output = produce(source, graph=g, target_storage=s, env=env)
    records = output.as_records()
    assert len(records) == 2
    with env.session_scope() as sess:
        assert sess.query(SnapLog).count() == 2
        assert sess.query(DataBlockLog).count() == 3
        pl = sess.query(SnapLog).order_by(SnapLog.completed_at.desc()).first()
        assert pl.node_key == source.key
        assert pl.graph_id == g.get_metadata_obj().hash
        assert pl.node_start_state == {"records_extracted": 2}
        assert pl.node_end_state == {"records_extracted": 6}
        assert pl.snap_key == source.snap.key
        assert pl.snap_params == cfg
        assert pl.error is None
        ns = sess.query(NodeState).filter(NodeState.node_key == pl.node_key).first()
        assert ns.state == {"records_extracted": 6}


def test_node_reset():
    env = get_env()
    g = Graph(env)
    s = env._local_python_storage
    # Initial graph
    N = 2 * 4
    source = g.create_node(customer_source, params={"total_records": N})
    accum = g.create_node("core.dataframe_accumulator", input=source)
    metrics = g.create_node(shape_metrics, input=accum)
    # Run first time
    produce(source, graph=g, target_storage=s, env=env)

    # Now reset node
    with env.session_scope() as sess:
        state = source.get_state(sess)
        assert state.state is not None
        source.reset(sess)
        state = source.get_state(sess)
        assert state is None

    output = produce(metrics, graph=g, target_storage=s, env=env)
    records = output.as_records()
    expected_records = [
        {"metric": "row_count", "value": 4},  # Just one run of source, not two
        {"metric": "col_count", "value": 3},
    ]
    assert records == expected_records


@Input("metrics", schema="Metric")
@Input("cust", schema="Customer")
def with_latest_metrics_no_ref(metrics: DataBlock, cust: DataBlock):
    m = metrics.as_dataframe()
    c = cust.as_dataframe()
    return pd.concat([m, c])


@Input("metrics", schema="Metric", reference=True)
@Input("cust", schema="Customer")
def with_latest_metrics(metrics: DataBlock, cust: DataBlock):
    m = metrics.as_dataframe()
    c = cust.as_dataframe()
    return pd.concat([m, c])


def test_ref_input():
    env = get_env()
    g = Graph(env)
    s = env._local_python_storage
    # Initial graph
    N = 2 * 4
    source = g.create_node(customer_source, params={"total_records": N})
    accum = g.create_node("core.dataframe_accumulator", input=source)
    metrics = g.create_node(shape_metrics, input=accum)
    join_ref = g.create_node(
        with_latest_metrics, inputs={"metrics": metrics, "cust": source}
    )
    join = g.create_node(
        with_latest_metrics_no_ref, inputs={"metrics": metrics, "cust": source}
    )
    # Run once, for one metrics output
    output = produce(metrics, graph=g, target_storage=s, env=env)

    # Both joins work
    output = env.run_node(join_ref, g, target_storage=s)
    assert output is not None
    output = env.run_node(join, g, target_storage=s)
    assert output is not None
    # Run source to create new customers, but NOT new metrics
    output = env.run_node(source, g, target_storage=s)
    # This time only ref will still have a metrics input
    output = env.run_node(join_ref, g, target_storage=s)
    assert output is not None
    output = env.run_node(join, g, target_storage=s)
    assert output is None  # Regular join has exhausted metrics
