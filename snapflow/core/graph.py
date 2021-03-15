from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    Union,
    cast,
)

import networkx as nx
from loguru import logger
from snapflow.core.metadata.orm import BaseModel
from snapflow.core.node import DeclaredNode, Node, NodeLike, node
from snapflow.core.snap import SnapLike
from snapflow.utils.common import md5_hash, remove_dupes
from sqlalchemy import Column, String
from sqlalchemy.sql.sqltypes import JSON
import strictyaml as yaml

if TYPE_CHECKING:
    from snapflow import Environment
    from snapflow.core.streams import StreamLike


class NodeDoesNotExist(KeyError):
    pass


class GraphMetadata(BaseModel):
    hash = Column(String(128), primary_key=True)
    adjacency = Column(JSON)

    def __repr__(self) -> str:
        return self._repr(hash=self.hash,)


class DeclaredGraph:
    def __init__(self, nodes: Iterable[DeclaredNode] = None):
        self._nodes: Dict[str, DeclaredNode] = {}
        if nodes:
            for n in nodes:
                self.add_node(n)

    def __str__(self):
        s = "Nodes:\n------\n" + "\n".join(self._nodes.keys())
        return s

    def node(
        self,
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
        dn = node(
            snap=snap,
            key=key,
            params=params,
            inputs=inputs,
            input=input,
            graph=graph,
            output_alias=output_alias,
            schema_translation=schema_translation,
            upstream=upstream,
        )
        self.add_node(dn)
        return dn

    create_node = node  # Legacy api

    def add_node(self, node: DeclaredNode):
        if node.key in self._nodes:
            raise KeyError(
                f"Duplicate node key `{node.key}`. Specify a distinct key for the node (key='unique_key')"
            )
        node.graph = self
        self._nodes[node.key] = node

    def remove_node(self, node: DeclaredNode):
        del self._nodes[node.key]

    def get_node(self, key: NodeLike) -> DeclaredNode:
        if isinstance(key, DeclaredNode):
            return key
        assert isinstance(key, str)
        return self._nodes[key]

    def has_node(self, key: str) -> bool:
        return key in self._nodes

    def all_nodes(self) -> Iterable[DeclaredNode]:
        return self._nodes.values()

    def instantiate(self, env: Environment) -> Graph:
        g = Graph(env)
        for dn in self.all_nodes():
            n = dn.instantiate(env, g)
            g.add_node(n)
        return g


graph = DeclaredGraph
DEFAULT_GRAPH = graph()


def hash_adjacency(adjacency: List[Tuple[str, Dict]]) -> str:
    return md5_hash(str(adjacency))


class Graph:
    def __init__(self, env: Environment, nodes: Iterable[Node] = None):
        self.env = env
        self._nodes: Dict[str, Node] = {}
        if nodes:
            for n in nodes:
                self.add_node(n)

    def __str__(self):
        s = "Nodes:\n------\n" + "\n".join(self._nodes.keys())
        return s

    def get_metadata_obj(self) -> GraphMetadata:
        adjacency = self.adjacency_list()
        return GraphMetadata(hash=hash_adjacency(adjacency), adjacency=adjacency)

    # TODO: duplicated code
    def node(
        self,
        snap: Union[SnapLike, str],
        key: Optional[str] = None,
        params: Dict[str, Any] = None,
        inputs: Dict[str, StreamLike] = None,
        input: StreamLike = None,
        graph: Optional[DeclaredGraph] = None,
        output_alias: Optional[str] = None,
        schema_translation: Optional[Dict[str, Union[Dict[str, str], str]]] = None,
        upstream: Union[StreamLike, Dict[str, StreamLike]] = None,  # TODO: DEPRECATED
    ) -> Node:
        dn = node(
            snap=snap,
            key=key,
            params=params,
            inputs=inputs,
            input=input,
            graph=graph,
            output_alias=output_alias,
            schema_translation=schema_translation,
            upstream=upstream,
        )
        n = dn.instantiate(self.env, self)
        self.add_node(n)
        return n

    create_node = node  # Legacy api

    def add_node(self, node: Node):
        if node.key in self._nodes:
            raise KeyError(f"Duplicate node key {node.key}")
        self._nodes[node.key] = node

    def remove_node(self, node: Node):
        del self._nodes[node.key]

    def get_node(self, key: NodeLike) -> Node:
        if isinstance(key, Node):
            return key
        if isinstance(key, DeclaredNode):
            key = key.key
        assert isinstance(key, str)
        return self._nodes[key]

    def has_node(self, key: str) -> bool:
        return key in self._nodes

    def all_nodes(self) -> Iterable[Node]:
        return self._nodes.values()

    def validate_graph(self) -> bool:
        # TODO
        #  validate node keys are valid
        #  validate snaps are valid
        #  validate types are valid
        #  etc
        return True

    def as_nx_graph(self) -> nx.DiGraph:
        g = nx.DiGraph()
        for n in self.all_nodes():
            g.add_node(n.key)
            inputs = n.declared_inputs
            for input_stream in inputs.values():
                for input_node_key in input_stream.stream.source_node_keys():
                    g.add_node(input_node_key)
                    g.add_edge(input_node_key, n.key)
            # TODO: self ref edge?
        return g

    def adjacency_list(self):
        return list(self.as_nx_graph().adjacency())

    def get_all_upstream_dependencies_in_execution_order(
        self, node: Node
    ) -> List[Node]:
        g = self.as_nx_graph()
        node_keys = self._get_all_upstream_dependencies_in_execution_order(g, node.key)
        return [self.get_node(name) for name in node_keys]

    def _get_all_upstream_dependencies_in_execution_order(
        self, g: nx.DiGraph, node: str
    ) -> List[str]:
        nodes = []
        for parent_node in g.predecessors(node):
            if parent_node == node:
                # Ignore self-ref cycles
                continue
            parent_deps = self._get_all_upstream_dependencies_in_execution_order(
                g, parent_node
            )
            nodes.extend(parent_deps)
        nodes.append(node)
        # May have added nodes twice, just keep first reference:
        return remove_dupes(nodes)

    def get_all_nodes_in_execution_order(self) -> List[Node]:
        g = self.as_nx_graph()
        return [self.get_node(name) for name in nx.topological_sort(g)]


def load_graph_from_dict(raw_graph: Dict[str, Any]) -> DeclaredGraph:
    """
    nodes:
      - key: node1
        snap: core.extract_csv
        output_dataset_name: csv1
        inputs:
          - input1
        params:
          path: "****"
    """
    raw_nodes = raw_graph["nodes"]
    g = DeclaredGraph()
    for r in raw_nodes:
        g.node(**r)
    return g


def graph_from_yaml(yml: str) -> DeclaredGraph:
    d = yaml.load(yml).data
    if isinstance(d, list):
        d = {"nodes": d}
    assert isinstance(d, dict)
    return load_graph_from_dict(d)
