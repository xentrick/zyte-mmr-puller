# Zyte MMR Puller

This project fetches Rocket League profile data and validates it against a Pydantic model.

The reference payload in this repo is:

- `json/standard_profile.json`

## JSON Structure Overview

The file follows this top-level shape:

```json
{
	"data": {
		"availableSegments": [],
		"expiryDate": "...",
		"metadata": {},
		"platformInfo": {},
		"segments": [],
		"userInfo": {}
	}
}
```

## `data` Object

### `availableSegments`

List of season or playlist segment descriptors that can be requested or displayed.

Each entry usually contains:

- `attributes.season`: season number.
- `metadata.name`: human-readable label.
- `type`: usually `"playlist"`.

### `expiryDate`

ISO-8601 timestamp that indicates when the payload expires.

### `metadata`

General profile metadata:

- `currentSeason`: latest season in the response.
- `lastUpdated.displayValue` and `lastUpdated.value`: update timestamps.
- `playerId`: platform-independent player id used by the source.

### `platformInfo`

Platform account identity information:

- `platformSlug` (example: `epic`)
- `platformUserHandle`
- `platformUserId`
- `platformUserIdentifier`
- optional fields such as `avatarUrl` and `additionalParameters`

### `segments`

Main stats collection. This array mixes different segment kinds, including:

- `overview` segments for lifetime totals.
- `playlist` segments for mode- and season-specific ranks.
- `peak-rating` segments for historical peaks.

Each segment includes:

- `attributes`: context like `season` and `playlistId`.
- `expiryDate`: segment expiry timestamp.
- `metadata.name`: segment display name.
- `stats`: dictionary where key is stat name (for example `rating`, `tier`, `wins`) and value is a metric object.
- `type`: segment classification.

### `userInfo`

Additional account-level data (badges, premium flags, social info, and related metadata).

## Stat Metric Shape (`segments[*].stats[*]`)

A stat metric is not fully uniform across all segment types. Common fields are:

- `displayType`
- `displayValue`
- `value`
- `metadata`
- optional: `category`, `displayCategory`, `displayName`, `percentile`, `rank`

The parser in this repo allows extra fields so it can handle future API additions.

## Validation in Code

The JSON is validated into `StandardProfile` (Pydantic) in `zyte/models.py` and parsed in `ZyteMMRPuller.pull_mmr` in `zyte/core.py`.
