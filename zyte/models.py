from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import AliasChoices, BaseModel, ConfigDict, Field


class ModelBase(BaseModel):
    # Keep parsing resilient to API additions while validating known fields.
    model_config = ConfigDict(extra="allow")


class SegmentAttributes(ModelBase):
    season: int


class NameMetadata(ModelBase):
    name: str = Field(validation_alias=AliasChoices("name", "playlistName"))


class AvailableSegment(ModelBase):
    attributes: SegmentAttributes
    metadata: NameMetadata
    type: str


class LastUpdated(ModelBase):
    displayValue: datetime
    value: datetime


class ProfileMetadata(ModelBase):
    currentSeason: int
    lastUpdated: LastUpdated
    playerId: int


class PlatformInfo(ModelBase):
    additionalParameters: Any | None = None
    avatarUrl: str | None = None
    platformSlug: str
    platformUserHandle: str
    platformUserId: str
    platformUserIdentifier: str


class StatMetric(ModelBase):
    category: str | None = None
    displayCategory: str | None = None
    displayName: str | None = None
    displayType: str | None = None
    displayValue: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    percentile: float | None = None
    rank: int | None = None
    value: Any | None = None


class Segment(ModelBase):
    attributes: dict[str, Any] = Field(default_factory=dict)
    expiryDate: datetime
    metadata: NameMetadata
    stats: dict[str, StatMetric] = Field(default_factory=dict)
    type: str | None = None


class StandardProfileData(ModelBase):
    availableSegments: list[AvailableSegment] = Field(default_factory=list)
    expiryDate: datetime
    metadata: ProfileMetadata
    platformInfo: PlatformInfo
    segments: list[Segment] = Field(default_factory=list)


class StandardProfile(ModelBase):
    data: StandardProfileData


class PlaylistMMRRow(ModelBase):
    playlist_id: int | None = None
    playlist_name: str
    games_played: int | float | None = None
    games_played_display: str | None = None
    rank_rating: int | float | None = None
    rank_rating_display: str | None = None
    rank_tier: str | None = None
    rank_tier_value: int | float | None = None
    rank_division: str | None = None
    rank_division_value: int | float | None = None


class CurrentMMRResult(ModelBase):
    season: int | None = None
    playlists: list[PlaylistMMRRow] = Field(default_factory=list)


class PeakMMRResult(PlaylistMMRRow):
    season: int


class PeakMMRByPlaylistResult(ModelBase):
    playlists: list[PeakMMRResult] = Field(default_factory=list)
