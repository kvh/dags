from __future__ import annotations

import enum
import traceback
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Union

from sqlalchemy.orm import Session, relationship
from sqlalchemy.orm.relationships import RelationshipProperty
from sqlalchemy.sql.functions import func
from sqlalchemy.sql.schema import Column, ForeignKey
from sqlalchemy.sql.sqltypes import JSON, DateTime, Enum, Integer, String

from dags.core.data_block import DataBlock, DataBlockMetadata
from dags.core.environment import Environment
from dags.core.metadata.orm import DAGS_METADATA_TABLE_PREFIX, BaseModel
from dags.core.pipe import Pipe, PipeLike, ensure_pipe, make_pipe, make_pipe_name
from dags.core.pipe_interface import PipeInterface
from loguru import logger

if TYPE_CHECKING:
    from dags.core.runnable import ExecutionContext
    from dags.core.streams import DataBlockStream
    from dags.core.graph import Graph


NodeLike = Union[str, "Node"]


def inputs_as_nodes(graph: Graph, inputs: Dict[str, NodeLike]):
    return {name: graph.get_any_node(dnl) for name, dnl in inputs.items()}


def create_node(
    graph: Graph,
    key: str,
    pipe: Union[PipeLike, str],
    inputs: Optional[Union[NodeLike, Dict[str, NodeLike]]] = None,
    upstream: Optional[Union[NodeLike, Dict[str, NodeLike]]] = None,  # Synonym
    dataset_name: Optional[str] = None,
    config: Optional[Dict[str, Any]] = None,
    schema_mapping: Optional[Dict[str, Union[Dict[str, str], str]]] = None,
    output_alias: Optional[str] = None,
):
    config = config or {}
    env = graph.env
    if isinstance(pipe, str):
        pipe = env.get_pipe(pipe)
    else:
        pipe = make_pipe(pipe)
    interface = pipe.get_interface(env)
    _declared_inputs: Dict[str, NodeLike] = {}
    n = Node(
        env=graph.env,
        graph=graph,
        key=key,
        pipe=pipe,
        config=config,
        _dataset_name=dataset_name,
        interface=interface,
        _declared_inputs=_declared_inputs,
        _declared_schema_mapping=schema_mapping,
        output_alias=output_alias,
    )
    inputs = inputs or upstream
    if inputs is not None:
        for i, v in interface.assign_inputs(inputs).items():
            n._declared_inputs[i] = v
    return n


@dataclass(frozen=True)
class Node:
    env: Environment
    graph: Graph
    key: str
    pipe: Pipe
    config: Dict[str, Any]
    interface: PipeInterface
    _declared_inputs: Dict[str, NodeLike]
    output_alias: Optional[str] = None
    _dataset_name: Optional[str] = None
    _declared_schema_mapping: Optional[Dict[str, Union[Dict[str, str], str]]] = None

    def __repr__(self):
        return f"<{self.__class__.__name__}(key={self.key}, pipe={self.pipe.key})>"

    def __hash__(self):
        return hash(self.key)

    def get_state(self, sess: Session) -> Optional[Dict]:
        state = sess.query(NodeState).filter(NodeState.node_key == self.key).first()
        if state:
            return state.state
        return None

    def get_dataset_node_key(self) -> str:
        return f"{self.key}__dataset"

    def get_dataset_name(self) -> str:
        return self._dataset_name or self.key

    def get_alias(self) -> str:
        if self.output_alias:
            return self.output_alias
        if self.output_is_dataset():
            return self.get_dataset_name()
        return f"{self.key}_latest"

    def output_is_dataset(self) -> bool:
        return False
        # return self.get_interface().output.data_format_class == "DataSet"

    def create_dataset_nodes(self) -> List[Node]:
        dfi = self.get_interface()
        if dfi.output is None:
            raise
        # if self.output_is_dataset():
        #     return []
        # TODO: how do we choose runtime? just using lang for now
        lang = self.pipe.source_code_language()
        if lang == "sql":
            df_accum = "core.sql_accumulator"
            df_dedupe = "core.sql_dedupe_unique_keep_newest_row"
        else:
            df_accum = "core.dataframe_accumulator"
            df_dedupe = "core.dataframe_dedupe_unique_keep_newest_row"
        accum = create_node(
            self.graph,
            key=f"{self.key}__accumulator",
            pipe=self.env.get_pipe(df_accum),
            upstream=self,
        )
        dedupe = create_node(
            self.graph,
            key=f"{self.key}__dedupe",
            pipe=self.env.get_pipe(df_dedupe),
            upstream=accum,
            output_alias=self.get_dataset_name(),
        )
        logger.debug(f"Adding DataSet nodes {[accum, dedupe]}")
        return [accum, dedupe]

    def get_interface(self) -> PipeInterface:
        return self.interface

    def get_compiled_input_nodes(self) -> Dict[str, Node]:
        # Just a convenience function
        return self.graph.get_declared_graph_with_dataset_nodes().get_compiled_inputs(
            self
        )

    def get_declared_input_nodes(self) -> Dict[str, Node]:
        return inputs_as_nodes(self.graph, self.get_declared_inputs())

    def get_declared_inputs(self) -> Dict[str, NodeLike]:
        return self._declared_inputs or {}

    def get_schema_mapping_for_input(self, input_name: str) -> Optional[Dict[str, str]]:
        if not self._declared_schema_mapping:
            return None
        v = list(self._declared_schema_mapping.values())[0]
        if isinstance(v, str):
            # Just one mapping, so should be one input
            assert len(self.interface.inputs) == 1
            return self._declared_schema_mapping
        if isinstance(v, dict):
            return self._declared_schema_mapping.get(input_name)
        raise TypeError(self._declared_schema_mapping)

    def as_stream(self) -> DataBlockStream:
        from dags.core.streams import DataBlockStream

        return DataBlockStream(upstream=self)

    def get_latest_output(self, ctx: ExecutionContext) -> Optional[DataBlock]:
        block = (
            ctx.metadata_session.query(DataBlockMetadata)
            .join(DataBlockLog)
            .join(PipeLog)
            .filter(
                DataBlockLog.direction == Direction.OUTPUT,
                PipeLog.node_key == self.key,
            )
            .order_by(DataBlockLog.created_at.desc())
            .first()
        )
        if block is None:
            return None
        return block.as_managed_data_block(ctx)


# def build_composite_nodes(n: Node) -> Iterable[Node]:
#     if not n.pipe.is_composite:
#         raise
#     nodes = []
#     # TODO: this just supports chains for now, not arbitrary sub-graph
#     # (hard to imagine totally unconstrained sub-graph, but if we restrict
#     # to one input and one output, straightforward to support arbitrary interior)
#     # Multiple inputs would take some thought. No concept of multiple outputs in Dags
#     raw_inputs = list(n.get_declared_inputs().values())
#     assert len(raw_inputs) == 1, "Composite pipes take one input"
#     input_node = raw_inputs[0]
#     created_nodes = {}
#     for fn in n.pipe.sub_graph:
#         fn = ensure_pipe(n.env, fn)
#         child_fn_name = make_pipe_name(fn)
#         child_node_key = f"{n.key}__{child_fn_name}"  # TODO: could be name clash since we don't include module name here
#         try:
#             if child_node_key in created_nodes:
#                 node = created_nodes[child_node_key]
#             else:
#                 node = n.graph.get_declared_node(child_node_key)
#         except KeyError:
#             node = create_node(
#                 graph=n.graph,
#                 key=child_node_key,
#                 pipe=fn,
#                 config=n.config,
#                 inputs=input_node,
#                 declared_composite_node_key=n.declared_composite_node_key
#                 or n.key,  # Handle nested composite pipes
#             )
#             created_nodes[node.key] = node
#         nodes.append(node)
#         input_node = node
#     return nodes


class NodeState(BaseModel):
    node_key = Column(String, primary_key=True)
    state = Column(JSON, nullable=True)

    def __repr__(self):
        return self._repr(
            node_key=self.node_key,
            state=self.state,
        )


def get_state(sess: Session, node_key: str) -> Optional[Dict]:
    state = sess.query(NodeState).filter(NodeState.node_key == node_key).first()
    if state:
        return state.state
    return None


class PipeLog(BaseModel):
    id = Column(Integer, primary_key=True, autoincrement=True)
    node_key = Column(
        String, nullable=False
    )  # TODO / FIXME: node_key is only unique to a graph now, not an env. Hmmmm
    node_start_state = Column(JSON, nullable=True)
    node_end_state = Column(JSON, nullable=True)
    pipe_key = Column(String, nullable=False)
    pipe_config = Column(JSON, nullable=True)
    runtime_url = Column(String, nullable=False)
    queued_at = Column(DateTime, nullable=True)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    error = Column(JSON, nullable=True)
    data_block_logs: RelationshipProperty = relationship(
        "DataBlockLog", backref="pipe_log"
    )

    def __repr__(self):
        return self._repr(
            id=self.id,
            node_key=self.node_key,
            pipe_key=self.pipe_key,
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
        state.state = self.node_end_state
        return sess.merge(state)


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
    pipe_log_id = Column(Integer, ForeignKey(PipeLog.id), nullable=False)
    data_block_id = Column(
        String,
        ForeignKey(f"{DAGS_METADATA_TABLE_PREFIX}data_block_metadata.id"),
        nullable=False,
    )  # TODO table name ref ugly here. We can parameterize with orm constant at least, or tablename("DataBlock.id")
    direction = Column(Enum(Direction, native_enum=False), nullable=False)
    processed_at = Column(DateTime, default=func.now(), nullable=False)
    # Hints
    data_block: "DataBlockMetadata"
    pipe_log: PipeLog

    def __repr__(self):
        return self._repr(
            id=self.id,
            pipe_log=self.pipe_log,
            data_block=self.data_block,
            direction=self.direction,
            processed_at=self.processed_at,
        )

    @classmethod
    def summary(cls, env: Environment) -> str:
        s = ""
        with env.session_scope() as sess:
            for dbl in sess.query(DataBlockLog).all():
                s += f"{dbl.pipe_log.node_key:50}"
                s += f"{str(dbl.data_block_id):23}"
                s += f"{str(dbl.data_block.record_count):6}"
                s += f"{dbl.direction.value:9}{str(dbl.data_block.updated_at):22}"
                s += f"{dbl.data_block.expected_schema_key:20}{dbl.data_block.realized_schema_key:20}\n"
        return s
