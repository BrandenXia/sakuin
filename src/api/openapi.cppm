module;

#include <nlohmann/json.hpp>

export module sakuin.api.openapi;

import std;

import sakuin.core.bytes;
import sakuin.core.result;
import sakuin.core.version;

export namespace sakuin::api {

core::Result<core::ByteBuffer> openapi_document();

} // namespace sakuin::api

namespace sakuin::api {

core::Result<core::ByteBuffer> openapi_document() {
  static const auto encoded = []() -> core::Result<core::ByteBuffer> {
    try {
      auto document = nlohmann::json::parse(R"json({
  "openapi": "3.1.2",
  "jsonSchemaDialect": "https://spec.openapis.org/oas/3.1/dialect/base",
  "info": {
    "title": "Sakuin API",
    "version": "dev",
    "description": "Native JSON search and operations API. Torznab XML discovery is available separately through /torznab/api?t=caps."
  },
  "servers": [{"url": "/"}],
  "tags": [
    {"name": "Discovery"},
    {"name": "Search"},
    {"name": "Observability"},
    {"name": "Operations"}
  ],
  "paths": {
    "/openapi.json": {
      "get": {
        "tags": ["Discovery"],
        "summary": "Get this native API description",
        "description": "/v1/openapi.json is an alias.",
        "operationId": "getOpenApiDocument",
        "responses": {
          "200": {"description": "OpenAPI description", "content": {"application/json": {"schema": {"type": "object"}}}}
        }
      }
    },
    "/v1/health": {
      "get": {
        "tags": ["Discovery"],
        "summary": "Check HTTP process liveness",
        "operationId": "getHealth",
        "responses": {
          "200": {
            "description": "The HTTP process is alive",
            "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Health"}}}
          }
        }
      }
    },
    "/v1/ready": {
      "get": {
        "tags": ["Discovery"],
        "summary": "Check composed service readiness",
        "operationId": "getReadiness",
        "responses": {
          "200": {
            "description": "The service and all enabled DHT workers are running",
            "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Readiness"}}}
          },
          "503": {
            "description": "The service is not ready",
            "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Readiness"}}}
          }
        }
      }
    },
    "/v1/search": {
      "get": {
        "tags": ["Search"],
        "summary": "Search indexed torrent metadata",
        "description": "Requires a credential with the search permission.",
        "operationId": "searchTorrents",
        "security": [{"bearerAuth": []}],
        "parameters": [
          {"$ref": "#/components/parameters/Query"},
          {"$ref": "#/components/parameters/MinimumSize"},
          {"$ref": "#/components/parameters/MaximumSize"},
          {"$ref": "#/components/parameters/MinimumFiles"},
          {"$ref": "#/components/parameters/MaximumFiles"},
          {"$ref": "#/components/parameters/FirstSeenAfter"},
          {"$ref": "#/components/parameters/LastSeenBefore"},
          {"$ref": "#/components/parameters/Category"},
          {"$ref": "#/components/parameters/ClassificationState"},
          {"$ref": "#/components/parameters/ContentKind"},
          {"$ref": "#/components/parameters/MinimumKindConfidence"},
          {"$ref": "#/components/parameters/ClassificationLabel"},
          {"$ref": "#/components/parameters/MinimumLabelConfidence"},
          {"$ref": "#/components/parameters/Offset"},
          {"$ref": "#/components/parameters/Limit"}
        ],
        "responses": {
          "200": {
            "description": "A bounded page of matching torrents",
            "content": {"application/json": {"schema": {"$ref": "#/components/schemas/SearchResult"}}}
          },
          "400": {"$ref": "#/components/responses/BadRequest"},
          "401": {"$ref": "#/components/responses/Unauthorized"},
          "403": {"$ref": "#/components/responses/Forbidden"},
          "429": {"$ref": "#/components/responses/RateLimited"}
        }
      }
    },
    "/v1/duplicates": {
      "get": {
        "tags": ["Search"],
        "summary": "List duplicate groups",
        "description": "Requires the search permission.",
        "operationId": "listDuplicateGroups",
        "security": [{"bearerAuth": []}],
        "parameters": [
          {
            "name": "algorithm",
            "in": "query",
            "required": true,
            "schema": {"type": "string", "enum": ["exact_file_layout_v1", "normalized_metadata_v1", "payload_layout_v1", "release_identity_v1"]}
          },
          {"name": "min_members", "in": "query", "schema": {"type": "integer", "minimum": 2, "default": 2}},
          {"$ref": "#/components/parameters/Offset"},
          {"name": "limit", "in": "query", "schema": {"type": "integer", "minimum": 1, "maximum": 200, "default": 50}}
        ],
        "responses": {
          "200": {
            "description": "A bounded page of duplicate groups",
            "content": {"application/json": {"schema": {"$ref": "#/components/schemas/DuplicateGroupsResult"}}}
          },
          "400": {"$ref": "#/components/responses/BadRequest"},
          "401": {"$ref": "#/components/responses/Unauthorized"},
          "403": {"$ref": "#/components/responses/Forbidden"},
          "404": {"$ref": "#/components/responses/NotFound"},
          "429": {"$ref": "#/components/responses/RateLimited"}
        }
      }
    },
    "/v1/duplicates/{infohash}": {
      "get": {
        "tags": ["Search"],
        "summary": "Find duplicate groups for one torrent",
        "description": "Requires the search permission.",
        "operationId": "getTorrentDuplicates",
        "security": [{"bearerAuth": []}],
        "parameters": [{"$ref": "#/components/parameters/InfoHash"}],
        "responses": {
          "200": {"description": "Duplicate memberships", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/DuplicateMatches"}}}},
          "400": {"$ref": "#/components/responses/BadRequest"},
          "401": {"$ref": "#/components/responses/Unauthorized"},
          "403": {"$ref": "#/components/responses/Forbidden"},
          "404": {"$ref": "#/components/responses/NotFound"},
          "429": {"$ref": "#/components/responses/RateLimited"}
        }
      }
    },
    "/v1/status": {
      "get": {
        "tags": ["Observability"],
        "summary": "Get detailed service status",
        "description": "Requires the admin permission.",
        "operationId": "getServiceStatus",
        "security": [{"bearerAuth": []}],
        "responses": {
          "200": {"description": "Current operational snapshot", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/ServiceStatus"}}}},
          "401": {"$ref": "#/components/responses/Unauthorized"},
          "403": {"$ref": "#/components/responses/Forbidden"},
          "404": {"$ref": "#/components/responses/NotFound"},
          "429": {"$ref": "#/components/responses/RateLimited"}
        }
      }
    },
    "/metrics": {
      "get": {
        "tags": ["Observability"],
        "summary": "Get Prometheus metrics",
        "description": "Requires the admin permission. /v1/metrics is an alias.",
        "operationId": "getPrometheusMetrics",
        "security": [{"bearerAuth": []}],
        "responses": {
          "200": {"description": "Prometheus text exposition", "content": {"text/plain": {"schema": {"type": "string"}}}},
          "401": {"$ref": "#/components/responses/Unauthorized"},
          "403": {"$ref": "#/components/responses/Forbidden"},
          "404": {"$ref": "#/components/responses/NotFound"},
          "429": {"$ref": "#/components/responses/RateLimited"}
        }
      }
    },
    "/v1/operations/search-refresh": {
      "post": {
        "tags": ["Operations"],
        "summary": "Refresh the derived search index",
        "description": "Requires the admin permission.",
        "operationId": "refreshSearchIndex",
        "security": [{"bearerAuth": []}],
        "responses": {
          "200": {"description": "Search refresh completed", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/SearchRefreshResult"}}}},
          "401": {"$ref": "#/components/responses/Unauthorized"},
          "403": {"$ref": "#/components/responses/Forbidden"},
          "404": {"$ref": "#/components/responses/NotFound"},
          "429": {"$ref": "#/components/responses/RateLimited"}
        }
      }
    },
    "/v1/operations/storage-maintenance": {
      "post": {
        "tags": ["Operations"],
        "summary": "Enqueue a storage-maintenance pass",
        "description": "Requires the admin permission. Requests are coalesced onto the maintenance owner thread.",
        "operationId": "requestStorageMaintenance",
        "security": [{"bearerAuth": []}],
        "parameters": [
          {"name": "verify", "in": "query", "schema": {"type": "boolean", "default": false}}
        ],
        "responses": {
          "202": {"description": "Maintenance request accepted", "headers": {"Location": {"schema": {"type": "string"}}}, "content": {"application/json": {"schema": {"$ref": "#/components/schemas/MaintenanceRequestResult"}}}},
          "400": {"$ref": "#/components/responses/BadRequest"},
          "401": {"$ref": "#/components/responses/Unauthorized"},
          "403": {"$ref": "#/components/responses/Forbidden"},
          "404": {"$ref": "#/components/responses/NotFound"},
          "429": {"$ref": "#/components/responses/RateLimited"}
        }
      }
    }
  },
  "components": {
    "securitySchemes": {
      "bearerAuth": {"type": "http", "scheme": "bearer", "bearerFormat": "Sakuin API key"}
    },
    "parameters": {
      "Query": {"name": "q", "in": "query", "schema": {"type": "string"}},
      "MinimumSize": {"name": "min_size", "in": "query", "schema": {"type": "integer", "minimum": 0}},
      "MaximumSize": {"name": "max_size", "in": "query", "schema": {"type": "integer", "minimum": 0}},
      "MinimumFiles": {"name": "min_files", "in": "query", "schema": {"type": "integer", "minimum": 0}},
      "MaximumFiles": {"name": "max_files", "in": "query", "schema": {"type": "integer", "minimum": 0}},
      "FirstSeenAfter": {"name": "first_seen_at_or_after_ms", "in": "query", "schema": {"type": "integer"}},
      "LastSeenBefore": {"name": "last_seen_at_or_before_ms", "in": "query", "schema": {"type": "integer"}},
      "Category": {"name": "category", "in": "query", "description": "Comma-separated semantic categories, matched with OR semantics.", "schema": {"type": "string", "examples": ["movie,movie_uhd", "series_anime", "game", "ebook"]}},
      "ClassificationState": {"name": "classification_state", "in": "query", "description": "Exact classifier lifecycle state.", "schema": {"type": "string", "enum": ["awaiting_metadata", "classified", "ambiguous", "unknown"]}},
      "ContentKind": {"name": "content_kind", "in": "query", "description": "Exact classifier content kind.", "schema": {"type": "string", "enum": ["unknown", "movie", "series", "music", "audiobook", "ebook", "game", "application", "mixed"]}},
      "MinimumKindConfidence": {"name": "minimum_kind_confidence", "in": "query", "schema": {"type": "string", "enum": ["low", "medium", "high"]}},
      "ClassificationLabel": {"name": "label", "in": "query", "description": "Comma-separated classifier labels. Every label must be present; Adult visibility policy still applies.", "schema": {"type": "string", "examples": ["anime", "adult,anime"]}},
      "MinimumLabelConfidence": {"name": "minimum_label_confidence", "in": "query", "description": "Minimum confidence for every requested label. Requires label; omitted means low.", "schema": {"type": "string", "enum": ["low", "medium", "high"]}},
      "Offset": {"name": "offset", "in": "query", "schema": {"type": "integer", "minimum": 0, "default": 0}},
      "Limit": {"name": "limit", "in": "query", "schema": {"type": "integer", "minimum": 1, "maximum": 1000, "default": 50}},
      "InfoHash": {"name": "infohash", "in": "path", "required": true, "schema": {"type": "string", "pattern": "^[0-9A-Fa-f]{40}$"}}
    },
    "responses": {
      "BadRequest": {"description": "Invalid request", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Error"}}}},
      "Unauthorized": {"description": "A valid API key is required", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Error"}}}},
      "Forbidden": {"description": "The credential lacks the required permission", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Error"}}}},
      "NotFound": {"description": "The route or optional subsystem is unavailable", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Error"}}}},
      "RateLimited": {"description": "The credential request limit was exceeded", "headers": {"Retry-After": {"schema": {"type": "integer", "minimum": 1}}}, "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Error"}}}}
    },
    "schemas": {
      "Error": {
        "type": "object",
        "required": ["error"],
        "properties": {"error": {"type": "object", "required": ["code", "message"], "properties": {"code": {"type": "string"}, "message": {"type": "string"}}}}
      },
      "Health": {"type": "object", "required": ["status"], "properties": {"status": {"const": "ok"}}},
      "Readiness": {"type": "object", "required": ["status"], "properties": {"status": {"type": "string", "enum": ["ready", "not_ready"]}}},
      "SearchHit": {
        "type": "object",
        "required": ["info_hash", "name", "total_size", "file_count", "first_seen_ms", "last_seen_ms", "score", "classification"],
        "properties": {
          "info_hash": {"type": "string", "pattern": "^[0-9a-f]{40}$"},
          "name": {"type": ["string", "null"]},
          "total_size": {"type": "integer", "minimum": 0},
          "file_count": {"type": "integer", "minimum": 0},
          "first_seen_ms": {"type": "integer"},
          "last_seen_ms": {"type": "integer"},
          "score": {"type": "integer", "minimum": 0},
          "classification": {"$ref": "#/components/schemas/Classification"}
        }
      },
      "Classification": {
        "type": "object",
        "required": ["state", "kind", "confidence", "labels", "categories", "evidence", "resolution", "algorithm_version", "input_truncated"],
        "properties": {
          "state": {"type": "string", "enum": ["awaiting_metadata", "classified", "ambiguous", "unknown"]},
          "kind": {"type": "string", "enum": ["unknown", "movie", "series", "music", "audiobook", "ebook", "game", "application", "mixed"]},
          "confidence": {"type": "string", "enum": ["unknown", "low", "medium", "high"]},
          "labels": {"type": "array", "items": {"type": "object", "required": ["name", "confidence"], "properties": {"name": {"type": "string", "enum": ["adult", "anime"]}, "confidence": {"type": "string", "enum": ["unknown", "low", "medium", "high"]}}}},
          "categories": {"type": "array", "items": {"type": "string", "enum": ["movie", "movie_sd", "movie_hd", "movie_uhd", "series", "series_sd", "series_hd", "series_uhd", "series_anime", "audio", "audiobook", "application", "game", "books", "ebook", "adult", "other"]}},
          "evidence": {"type": "array", "items": {"$ref": "#/components/schemas/ClassificationEvidence"}},
          "resolution": {"type": ["string", "null"], "enum": ["sd", "720p", "1080p", "2160p", null]},
          "algorithm_version": {"type": "integer", "minimum": 1},
          "input_truncated": {"type": "boolean"}
        }
      },
      "ClassificationEvidence": {
        "type": "object",
        "required": ["code", "subject", "weight"],
        "properties": {
          "code": {"type": "string", "enum": ["video_payload_dominant", "audio_payload_dominant", "ebook_payload_dominant", "game_payload_dominant", "application_payload_dominant", "multiple_payload_families", "single_dominant_video", "season_episode_token", "release_year_token", "music_release_token", "audiobook_token", "ebook_token", "game_token", "application_token", "fuzzy_semantic_token", "learned_content_model", "operator_rule", "adult_token", "anime_token", "resolution_token"]},
          "subject": {"type": "string", "enum": ["movie", "series", "music", "audiobook", "ebook", "game", "application", "mixed", "adult", "anime", "resolution"]},
          "weight": {"type": "integer"},
          "rule_id": {"type": "string", "description": "Present only for a matched operator rule."}
        }
      },
      "SearchResult": {
        "type": "object",
        "required": ["source_generation", "total_matches", "hits"],
        "properties": {
          "source_generation": {"type": "integer", "minimum": 0},
          "total_matches": {"type": "integer", "minimum": 0},
          "hits": {"type": "array", "items": {"$ref": "#/components/schemas/SearchHit"}}
        }
      },
      "DuplicateGroup": {
        "type": "object",
        "required": ["algorithm", "fingerprint", "torrents"],
        "properties": {
          "algorithm": {"type": "string", "enum": ["exact_file_layout_v1", "normalized_metadata_v1"]},
          "fingerprint": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
          "torrents": {"type": "array", "items": {"type": "string", "pattern": "^[0-9a-f]{40}$"}}
        }
      },
      "DuplicateGroupsResult": {
        "type": "object",
        "required": ["source_generation", "total_groups", "groups"],
        "properties": {
          "source_generation": {"type": "integer", "minimum": 0},
          "total_groups": {"type": "integer", "minimum": 0},
          "groups": {"type": "array", "items": {"$ref": "#/components/schemas/DuplicateGroup"}}
        }
      },
      "DuplicateMatches": {
        "type": "object",
        "required": ["source_generation", "info_hash", "groups"],
        "properties": {
          "source_generation": {"type": "integer", "minimum": 0},
          "info_hash": {"type": "string", "pattern": "^[0-9a-f]{40}$"},
          "groups": {"type": "array", "items": {"$ref": "#/components/schemas/DuplicateGroup"}}
        }
      },
      "SearchRefreshResult": {
        "type": "object",
        "required": ["operation", "status"],
        "properties": {"operation": {"const": "search_refresh"}, "status": {"const": "completed"}}
      },
      "MaintenanceRequestResult": {
        "type": "object",
        "required": ["operation", "status", "verification"],
        "properties": {"operation": {"const": "storage_maintenance"}, "status": {"const": "accepted"}, "verification": {"type": "boolean"}}
      },
      "ClassificationIndexStats": {
        "type": "object",
        "required": ["enabled", "algorithm_version", "total_records", "estimated_memory_bytes", "states", "input_truncated", "adult_labeled", "learned", "categories"],
        "properties": {
          "enabled": {"type": "boolean"},
          "algorithm_version": {"type": "integer", "minimum": 1},
          "total_records": {"type": "integer", "minimum": 0},
          "estimated_memory_bytes": {"type": "integer", "minimum": 0},
          "states": {
            "type": "object",
            "required": ["awaiting_metadata", "classified", "ambiguous", "unknown"],
            "additionalProperties": false,
            "properties": {
              "awaiting_metadata": {"type": "integer", "minimum": 0},
              "classified": {"type": "integer", "minimum": 0},
              "ambiguous": {"type": "integer", "minimum": 0},
              "unknown": {"type": "integer", "minimum": 0}
            }
          },
          "input_truncated": {"type": "integer", "minimum": 0},
          "adult_labeled": {"type": "integer", "minimum": 0},
          "learned": {
            "type": "object",
            "required": ["enabled", "ready", "training_records", "classified_records", "eligible_kinds", "vocabulary_size"],
            "additionalProperties": false,
            "properties": {
              "enabled": {"type": "boolean"},
              "ready": {"type": "boolean"},
              "training_records": {"type": "integer", "minimum": 0},
              "classified_records": {"type": "integer", "minimum": 0},
              "eligible_kinds": {"type": "integer", "minimum": 0},
              "vocabulary_size": {"type": "integer", "minimum": 0}
            }
          },
          "categories": {"type": "object", "additionalProperties": {"type": "integer", "minimum": 0}}
        }
      },
      "SearchStatus": {
        "type": "object",
        "required": ["source_generation", "records_indexed", "classification"],
        "properties": {
          "source_generation": {"type": "integer", "minimum": 0},
          "records_indexed": {"type": "integer", "minimum": 0},
          "classification": {"$ref": "#/components/schemas/ClassificationIndexStats"}
        }
      },
      "ServiceErrorStatus": {
        "type": "object",
        "required": ["source", "message", "count", "last_seen_ms", "active", "recovered_at_ms"],
        "properties": {
          "source": {"type": "string"},
          "message": {"type": "string"},
          "count": {"type": "integer", "minimum": 1},
          "last_seen_ms": {"type": "integer"},
          "active": {"type": "boolean"},
          "recovered_at_ms": {"type": ["integer", "null"]}
        }
      },
      "ServiceErrors": {
        "type": "object",
        "required": ["total", "active", "sources"],
        "properties": {
          "total": {"type": "integer", "minimum": 0},
          "active": {"type": "integer", "minimum": 0},
          "sources": {"type": "array", "items": {"$ref": "#/components/schemas/ServiceErrorStatus"}}
        }
      },
      "ServiceStatus": {
        "type": "object",
        "required": ["version", "state", "started_at_ms", "uptime_ms", "dht", "search", "materialization", "duplicates", "maintenance", "service_errors", "last_service_error"],
        "properties": {
          "version": {"type": "string"},
          "state": {"type": "string"},
          "started_at_ms": {"type": "integer"},
          "uptime_ms": {"type": "integer", "minimum": 0},
          "dht": {"type": "object"},
          "search": {"$ref": "#/components/schemas/SearchStatus"},
          "materialization": {"type": "object"},
          "duplicates": {"type": "object"},
          "maintenance": {"type": "object"},
          "service_errors": {"$ref": "#/components/schemas/ServiceErrors"},
          "last_service_error": {"oneOf": [{"$ref": "#/components/schemas/ServiceErrorStatus"}, {"type": "null"}]}
        }
      }
    }
  }
})json");
      document["info"]["version"] = core::version;
      const auto text = document.dump();
      const auto bytes = std::as_bytes(std::span{text});
      return core::ByteBuffer{bytes.begin(), bytes.end()};
    } catch (const std::exception &exception) {
      return std::unexpected(
          core::Error{core::ErrorCode::Internal,
                      std::string{"Could not serialize OpenAPI document: "} +
                          exception.what()});
    }
  }();
  return encoded;
}

} // namespace sakuin::api
