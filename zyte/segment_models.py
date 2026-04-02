from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import AliasChoices, BaseModel, ConfigDict, Field


class SegmentModelBase(BaseModel):
    model_config = ConfigDict(extra="allow")


class SegmentPlaylistAttributes(SegmentModelBase):
    playlistId: int | None = None
    season: int | None = None


class SegmentPlaylistMetadata(SegmentModelBase):
    name: str = Field(validation_alias=AliasChoices("name", "playlistName"))


class SegmentPlaylistStat(SegmentModelBase):
    displayName: str | None = None
    displayCategory: str | None = None
    category: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    value: Any | None = None
    displayValue: str | None = None
    displayType: str | None = None
    percentile: float | None = None
    rank: int | None = None


class SegmentPlaylistEntry(SegmentModelBase):
    type: str | None = None
    attributes: SegmentPlaylistAttributes = Field(
        default_factory=SegmentPlaylistAttributes
    )
    metadata: SegmentPlaylistMetadata
    expiryDate: datetime
    stats: dict[str, SegmentPlaylistStat] = Field(default_factory=dict)


class SegmentPlaylistResponse(SegmentModelBase):
    data: list[SegmentPlaylistEntry] = Field(default_factory=list)
