from __future__ import annotations

import enum
import traceback
from dataclasses import dataclass, field
from operator import and_
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Union

from loguru import logger
from snapflow.core.data_block import DataBlock, DataBlockMetadata
from snapflow.core.environment import Environment
from snapflow.core.metadata.orm import SNAPFLOW_METADATA_TABLE_PREFIX, BaseModel
from snapflow.core.snap import SnapLike, _Snap, ensure_snap, make_snap, make_snap_name
from snapflow.core.snap_interface import DeclaredSnapInterface, DeclaredStreamInput
from snapflow.storage.storage import SqliteStorageEngine
from snapflow.utils.common import as_identifier
from sqlalchemy.orm import Session, relationship
from sqlalchemy.orm.relationships import RelationshipProperty
from sqlalchemy.sql.functions import func
from sqlalchemy.sql.schema import Column, ForeignKey
from sqlalchemy.sql.sqltypes import JSON, Boolean, DateTime, Enum, Integer, String

if TYPE_CHECKING:
    from snapflow.core.execution import RunContext
    from snapflow.core.streams import StreamBuilder, StreamLike
    from snapflow.core.graph import Graph, GraphMetadata, DeclaredGraph, DEFAULT_GRAPH

NodeLike = Union[str, "Node", "DeclaredNode"]
NodeBase = Union["Node", "DeclaredNode"]


def ensure_stream(stream_like: StreamLike) -> StreamBuilder:
    from snapflow.core.streams import StreamBuilder, StreamLike

    if isinstance(stream_like, StreamBuilder):
        return stream_like
    if isinstance(stream_like, DeclaredNode) or isinstance(stream_like, Node):
        return stream_like.as_stream_builder()
    if isinstance(stream_like, str):
        return StreamBuilder().filter_inputs([stream_like])
    raise TypeError(stream_like)


@dataclass
class DeclaredNode:
    """
    Node as it is declared, which may be before
    we have loaded necessary modules, or seen the full graph,
    so we will not have schema or snap definitions, will not know the
    interfaces, and will not be able to hook up the graph
    yet. Only after a call to `instantiate_node` do we then do these things.
    """

    snap: Union[SnapLike, str]
    key: str
    params: Dict[str, Any] = field(default_factory=dict)
    inputs: Union[StreamLike, Dict[str, StreamLike]] = field(default_factory=dict)
    graph: Optional[DeclaredGraph] = None
    output_alias: Optional[str] = None
    schema_translation: Optional[Dict[str, Union[Dict[str, str], str]]] = None

    def __post_init__(self):
        from snapflow.core.graph import DEFAULT_GRAPH

        # Ensure node is in graph (or put in default graph)
        # TODO: better way to do this?
        if self.graph is None:
            self.graph = DEFAULT_GRAPH
        # self.graph.add_node(self)

    def __repr__(self):
        return f"<{self.__class__.__name__}(key={self.key}, snap={self.snap})>"

    def __hash__(self):
        return hash(self.key)

    def set_inputs(self, *args, **kwargs):
        """If a single positional argument, overwrites inputs.
        If kwargs then updates any existing inputs.
        """
        if args:
            if len(args) > 1 or kwargs:
                raise Exception("Provide keyword args for multiple inputs")
            self.inputs = args[0]
        else:
            if isinstance(self.inputs, dict):
                self.inputs.update(kwargs)
            else:
                self.inputs = kwargs

    def set_input(self, input: StreamLike = None, **kwargs):
        if input:
            self.set_inputs(input)
        elif kwargs:
            self.set_inputs(**kwargs)
        else:
            raise ValueError()

    def as_stream_builder(self) -> StreamBuilder:
        from snapflow.core.streams import StreamBuilder

        return StreamBuilder().filter_inputs(self)

    def instantiate(self, env: Environment, g: Graph = None) -> Node:
        from snapflow.core.graph import Graph

        if g is None:
            g = Graph(env)
        return instantiate_node(env, g, self)


def node(
    snap: Union[SnapLike, str],
    key: Optional[str] = None,
    params: Dict[str, Any] = None,
    inputs: Dict[str, StreamLike] = None,
    input: StreamLike = None,
    graph: Optional[DeclaredGraph] = None,
    output_alias: Optional[str] = None,
    schema_translation: Optional[Dict[str, Union[Dict[str, str], str]]] = None,
    upstream: Union[StreamLike, Dict[str, StreamLike]] = None,  # TODO: DEPRECATED
) -> DeclaredNode:
    if key is None:
        key = make_snap_name(snap)
    return DeclaredNode(
        snap=snap,
        key=key,
        params=params or {},
        inputs=inputs or upstream or input or {},
        graph=graph,
        output_alias=output_alias,
        schema_translation=schema_translation,
    )


def instantiate_node(
    env: Environment, graph: Graph, declared_node: DeclaredNode,
):
    if isinstance(declared_node.snap, str):
        snap = env.get_snap(declared_node.snap)
    else:
        snap = make_snap(declared_node.snap)
    interface = snap.get_interface()
    schema_translation = interface.assign_translations(declared_node.schema_translation)
    declared_inputs: Dict[str, DeclaredStreamInput] = {}
    if declared_node.inputs is not None:
        for name, stream_like in interface.assign_inputs(declared_node.inputs).items():
            declared_inputs[name] = DeclaredStreamInput(
                stream=ensure_stream(stream_like),
                declared_schema_translation=(schema_translation or {}).get(name),
            )
    n = Node(
        env=env,
        graph=graph,
        key=declared_node.key,
        snap=snap,
        params=declared_node.params,
        interface=interface,
        declared_inputs=declared_inputs,
        declared_schema_translation=schema_translation,
        output_alias=declared_node.output_alias,
    )
    return n


def make_default_output_alias(node: Node) -> str:
    return as_identifier(
        f"_{node.key}__latest"
    )  # TODO: as_identifier is storage-specific


@dataclass(frozen=True)
class Node:
    env: Environment
    graph: Graph
    key: str
    snap: _Snap
    params: Dict[str, Any]
    interface: DeclaredSnapInterface
    declared_inputs: Dict[str, DeclaredStreamInput]
    output_alias: Optional[str] = None
    declared_schema_translation: Optional[Dict[str, Dict[str, str]]] = None

    def __repr__(self):
        return f"<{self.__class__.__name__}(key={self.key}, snap={self.snap.key})>"

    def __hash__(self):
        return hash(self.key)

    def get_state(self, sess: Session) -> Optional[NodeState]:
        return sess.query(NodeState).filter(NodeState.node_key == self.key).first()

    def get_alias(self) -> str:
        if self.output_alias:
            ident = self.output_alias
        else:
            ident = make_default_output_alias(self)
        return as_identifier(
            ident
        )  # TODO: this logic should be storage api specific! and then shared back?

    def get_interface(self) -> DeclaredSnapInterface:
        return self.interface

    def get_schema_translation_for_input(
        self, input_name: str
    ) -> Optional[Dict[str, str]]:
        return (self.declared_schema_translation or {}).get(input_name)

    def as_stream_builder(self) -> StreamBuilder:
        from snapflow.core.streams import StreamBuilder

        return StreamBuilder().filter_inputs(self)

    def latest_output(self, ctx: RunContext, sess: Session) -> Optional[DataBlock]:
        block: DataBlockMetadata = (
            sess.query(DataBlockMetadata)
            .join(DataBlockLog)
            .join(SnapLog)
            .filter(
                DataBlockLog.direction == Direction.OUTPUT,
                SnapLog.node_key == self.key,
            )
            .order_by(DataBlockLog.created_at.desc())
            .first()
        )
        if block is None:
            return None
        return block.as_managed_data_block(ctx, sess)

    def _reset_state(self, sess: Session):
        """
        Resets the node's state only.
        This is usually not something you want to do by itself, but
        instead as part of a full reset.
        """
        state = self.get_state(sess)
        sess.delete(state)

    def _invalidate_output_datablocks(self, sess: Session):
        """
        TODO: would be better to invalidate / deactivate here than to actually delete?
        We're orphaning datablocks this way.
        """
        dbl_ids = [
            r[0]
            for r in sess.query(DataBlockLog.id)
            .join(SnapLog)
            .filter(
                SnapLog.node_key == self.key, DataBlockLog.direction == Direction.OUTPUT
            )
            .all()
        ]
        sess.query(DataBlockLog).filter(DataBlockLog.id.in_(dbl_ids)).update(
            {"invalidated": True}, synchronize_session=False
        )

    def reset(self, sess: Session):
        """
        Resets the node, meaning all state is cleared, and all OUTPUT datablock
        logs are invalidated. Output datablocks are NOT deleted.
        NB: If downstream nodes have already processed an output datablock,
        this will have no effect on them.
        TODO: consider "cascading reset" for downstream recursive snaps (which will still have
        accumulated output from this node)
        """
        self._reset_state(sess)
        self._invalidate_output_datablocks(sess)


class NodeState(BaseModel):
    node_key = Column(String(128), primary_key=True)
    state = Column(JSON, nullable=True)

    def __repr__(self):
        return self._repr(node_key=self.node_key, state=self.state,)


def get_state(sess: Session, node_key: str) -> Optional[Dict]:
    state = sess.query(NodeState).filter(NodeState.node_key == node_key).first()
    if state:
        return state.state
    return None


class SnapLog(BaseModel):
    id = Column(Integer, primary_key=True, autoincrement=True)
    graph_id = Column(
        String(128),
        ForeignKey(f"{SNAPFLOW_METADATA_TABLE_PREFIX}graph_metadata.hash"),
        nullable=False,
    )
    node_key = Column(String(128), nullable=False)
    node_start_state = Column(JSON, nullable=True)
    node_end_state = Column(JSON, nullable=True)
    snap_key = Column(String(128), nullable=False)
    snap_params = Column(JSON, nullable=True)
    runtime_url = Column(String(128), nullable=False)
    queued_at = Column(DateTime, nullable=True)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    error = Column(JSON, nullable=True)
    data_block_logs: RelationshipProperty = relationship(
        "DataBlockLog", backref="snap_log"
    )
    graph: "GraphMetadata"

    def __repr__(self):
        return self._repr(
            id=self.id,
            graph_id=self.graph_id,
            node_key=self.node_key,
            snap_key=self.snap_key,
            runtime_url=self.runtime_url,
            started_at=self.started_at,
        )

    def output_data_blocks(self) -> Iterable[DataBlockMetadata]:
        return [
            dbl for dbl in self.data_block_logs if dbl.direction == Direction.OUTPUT
        ]

    def input_data_blocks(self) -> Iterable[DataBlockMetadata]:
        return [dbl for dbl in self.data_block_logs if dbl.direction == Direction.INPUT]

    def set_error(self, e: Exception):
        tback = traceback.format_exc()
        # Traceback can be v large (like in max recursion), so we truncate to 5k chars
        self.error = {"error": str(e), "traceback": tback[:5000]}

    def persist_state(self, sess: Session) -> NodeState:
        state = (
            sess.query(NodeState).filter(NodeState.node_key == self.node_key).first()
        )
        if state is None:
            state = NodeState(node_key=self.node_key)
            sess.add(state)
        state.state = self.node_end_state
        sess.flush([state])
        return state


class Direction(enum.Enum):
    INPUT = "input"
    OUTPUT = "output"

    @property
    def symbol(self):
        if self.value == "input":
            return "←"
        return "➞"

    @property
    def display(self):
        s = "out"
        if self.value == "input":
            s = "in"
        return self.symbol + " " + s


class DataBlockLog(BaseModel):
    id = Column(Integer, primary_key=True, autoincrement=True)
    snap_log_id = Column(Integer, ForeignKey(SnapLog.id), nullable=False)
    data_block_id = Column(
        String(128),
        ForeignKey(f"{SNAPFLOW_METADATA_TABLE_PREFIX}data_block_metadata.id"),
        nullable=False,
    )
    direction = Column(Enum(Direction, native_enum=False), nullable=False)
    processed_at = Column(DateTime, default=func.now(), nullable=False)
    invalidated = Column(Boolean, default=False)
    # Hints
    data_block: "DataBlockMetadata"
    snap_log: SnapLog

    def __repr__(self):
        return self._repr(
            id=self.id,
            snap_log=self.snap_log,
            data_block=self.data_block,
            direction=self.direction,
            processed_at=self.processed_at,
        )

    @classmethod
    def summary(cls, sess: Session) -> str:
        s = ""
        for dbl in sess.query(DataBlockLog).all():
            s += f"{dbl.snap_log.node_key:50}"
            s += f"{str(dbl.data_block_id):23}"
            s += f"{str(dbl.data_block.record_count):6}"
            s += f"{dbl.direction.value:9}{str(dbl.data_block.updated_at):22}"
            s += f"{dbl.data_block.nominal_schema_key:20}{dbl.data_block.realized_schema_key:20}\n"
        return s
