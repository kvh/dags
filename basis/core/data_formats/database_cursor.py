from __future__ import annotations

from typing import Any, Dict, Generic, List, Optional, Type, TypeVar

from sqlalchemy.engine import ResultProxy

from basis.core.data_formats.base import DataFormatBase, MemoryDataFormatBase


class DatabaseCursorFormat(MemoryDataFormatBase):
    @classmethod
    def empty(cls):
        raise NotImplementedError

    @classmethod
    def type(cls) -> Type:
        return (
            ResultProxy  # TODO: assumes we are always using sqlalchemy. Which we are?
        )

    @classmethod
    def type_hint(cls):
        return "DatabaseCursor"

    @classmethod
    def copy_records(cls, obj: Any) -> Any:
        # Not applicable to cursor
        return obj


DatabaseCursor = TypeVar("DatabaseCursor", bound=ResultProxy)