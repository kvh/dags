from __future__ import annotations

from snapflow.core.environment import Environment
from snapflow.core.graph import Graph


def test_env_init():
    from . import _test_module

    # Test module / components
    env = Environment("_test", metadata_storage="sqlite://", modules=[])
    with env.session_scope() as sess:
        assert len(env.get_module_order()) == 1
        env.add_module(_test_module)
        assert env.get_module_order() == [
            env.get_local_module().name,
            _test_module.name,
        ]
        assert env.get_schema("TestSchema", sess) is _test_module.schemas.TestSchema
        assert env.get_snap("test_sql") is _test_module.snaps.test_sql
        # Test runtime / storage
        env.add_storage("postgresql://test")
        assert len(env.storages) == 2  # added plus default local memory
        assert len(env.runtimes) == 2  # added plus default local python
