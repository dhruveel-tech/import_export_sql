"""
Fabric Data Client - Direct MongoDB Integration
"""
from typing import Dict, List, Any, Optional
from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorClient
import httpx

from app.core.config import settings

from app.core.logging_config import logger


class FabricClient:
    """Async client for fetching and ingesting Fabric data in MongoDB."""

    def __init__(self) -> None:
        self.client = AsyncIOMotorClient(settings.MONGODB_URL)
        self.db = self.client[settings.MONGODB_DB_NAME]

    async def close(self) -> None:
        if self.client:
            self.client.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    def _collection(self, name: str):
        return self.db[name]

    def _to_object_id(self, value: Any) -> Any:
        if isinstance(value, str):
            try:
                return ObjectId(value)
            except Exception:
                return value
        return value

    # ------------------------------------------------------------------
    # FETCH METHODS
    # ------------------------------------------------------------------

    async def get_transcript(self, repo_guid: str, inputs: Optional[Dict] = None) -> Optional[Dict]:
        try:
            col = self._collection(settings.MONGODB_COLLECTION_NAME_FOR_GET_DATA)

            transcript_ids = inputs.get("event_ids", []) if inputs else []

            query: Dict[str, Any] = {"sdnaEventType": "transcript"}

            if transcript_ids:
                query["_id"] = {"$in": [self._to_object_id(i) for i in transcript_ids]}

            cursor = col.find(query)

            segments: List[Dict] = []
            async for item in cursor:
                segments.append(
                    {
                        "id": str(item["_id"]),
                        "sdnaEventType": item.get("sdnaEventType", ""),
                        "eventValue": item.get("eventValue", ""),
                        "start": item.get("start", 0),
                        "end": item.get("end", 0),
                        "fullPath": item.get("fullPath", ""),
                    }
                )

            if not segments:
                logger.warning(f"No transcript segments found for : repo_guid={repo_guid}")
                return None

            segments.sort(key=lambda x: x["start"])

            logger.info(f"Transcript fetched successfully for : repo_guid={repo_guid}, count={len(segments)}")

            return {"repo_guid": repo_guid, "segments": segments}

        except Exception as exc:
            logger.error(f"Transcript fetch failed for : repo_guid={repo_guid}, error={exc}", exc_info=True)
            return {"repo_guid": repo_guid, "segments": []}

    async def get_events(self, repo_guid: str, inputs: Optional[Dict] = None) -> Dict:
        try:
            query: Dict[str, Any] = {}
            col = self._collection(settings.MONGODB_COLLECTION_NAME_FOR_GET_DATA)

            ids = inputs.get("event_ids", []) if inputs else []

            query["sdnaEventType"] = {"$nin": ["transcript", "comment"]}
            if ids:
                query["_id"] = {"$in": [self._to_object_id(i) for i in ids]}

            cursor = col.find(query)

            events: List[Dict] = []
            async for item in cursor:
                events.append(
                    {
                        "id": str(item["_id"]),
                        "sdnaEventType": item.get("sdnaEventType", ""),
                        "eventValue": item.get("eventValue", ""),
                        "start": item.get("start", 0),
                        "end": item.get("end", 0),
                        "confidenceScore": item.get("confidenceScore", 0.0),
                        "positions": item.get("positions", []),
                        "fullPath": item.get("fullPath", ""),
                    }
                )

            events.sort(key=lambda x: x["start"])

            logger.info(f"Events fetched successfully for : repo_guid={repo_guid}, count={len(events)}")

            return {"repo_guid": repo_guid, "segments": events}

        except Exception as exc:
            logger.error(f"Events fetch failed for : repo_guid={repo_guid}, error={exc}", exc_info=True)
            return {"repo_guid": repo_guid, "segments": []}

    async def get_comments(self, repo_guid: str, inputs: Optional[Dict] = None) -> Dict:
        try:
            col = self._collection(settings.MONGODB_COLLECTION_NAME_FOR_GET_DATA)

            ids = inputs.get("event_ids", []) if inputs else []

            query: Dict[str, Any] = {"sdnaEventType": "comment"}

            if ids:
                query["_id"] = {"$in": [self._to_object_id(i) for i in ids]}

            cursor = col.find(query)

            comments: List[Dict] = []
            async for item in cursor:
                comments.append(
                    {
                        "id": str(item["_id"]),
                        "sdnaEventType": item.get("sdnaEventType", ""),
                        "eventValue": item.get("eventValue", ""),
                        "start": item.get("start", 0),
                        "end": item.get("end", 0),
                        "source": item.get("source", ""),
                        "fullPath": item.get("fullPath", ""),
                    }
                )

            comments.sort(key=lambda x: x["start"])

            logger.info(f"Comments fetched successfully for : repo_guid={repo_guid}, count={len(comments)}")

            return {"repo_guid": repo_guid, "segments": comments}

        except Exception as exc:
            logger.error(f"Comments fetch failed for : repo_guid={repo_guid}, error={exc}", exc_info=True)
            return {"repo_guid": repo_guid, "segments": []}

    # ------------------------------------------------------------------
    # INGEST METHODS
    # ------------------------------------------------------------------

    async def ingest_llm_highlights(
        self,
        repo_guid: str,
        full_path: str,
        highlights: list[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Send LLM highlights to Node API in batches.
        """

        created_count = 0
        skipped_count = 0

        if not highlights:
            return {"created": 0, "updated": 0, "skipped": 0, "total": 0}

        url = f"{settings.FABRIC_API_URL}/catalogs/aiEnrichedMetadata/insights/llm/add"
        file_name = full_path.split("/")[-1] if full_path else ""
        headers = {"apiKey": settings.FABRIC_API_KEY}

        BATCH_SIZE = 500  # ⭐ safe batch size for Node + network
        statu_msg = None
        async with httpx.AsyncClient(timeout=settings.FABRIC_API_TIMEOUT) as client:
            for i in range(0, len(highlights), BATCH_SIZE):
                batch = highlights[i : i + BATCH_SIZE]

                custom_events = []
                for h in batch:
                    try:
                        custom_events.append(
                            {
                                "insight": h.get("insight", ""),
                                "start": h.get("start", ""),
                                "end": h.get("end", ""),
                                "confidenceScore": h.get("confidenceScore", 0),
                                "eventMeta": {
                                    "associatedEventIds": h.get("eventMeta", {}).get(
                                        "associatedEventIds", []
                                    )
                                },
                            }
                        )
                    except Exception:
                        skipped_count += 1
                        logger.exception("Failed to parse highlight event, skipping.")

                if not custom_events:
                    continue

                node_payload = {
                    "repoGuid": repo_guid,
                    "fullPath": full_path,
                    "fileName": file_name,
                    "insightEvents": custom_events,
                }
                try:
                    response = await client.post(url, json=node_payload, headers=headers)

                    if response.is_success:
                        created_count += len(custom_events)
                        statu_msg = "success"
                    else:
                        skipped_count += len(custom_events)
                        logger.error(
                            "Node API failure :- status=%s , body=%s",
                            response.status_code,
                            response.text,
                        )
                        statu_msg = f"Node API failure : status={response.status_code} , body={response.text}"

                except httpx.HTTPError as e:
                    skipped_count += len(custom_events)
                    statu_msg = f"Node API failure : {e}"
                    logger.exception(f"HTTP error during Node API call: {e}")

        logger.info(
            "LLM highlights sent to Node API : created=%s , skipped=%s",
            created_count,
            skipped_count,
        )

        return {
            "created": created_count,
            "updated": 0,
            "skipped": skipped_count,
            "total": created_count + skipped_count,
            "error_msg":statu_msg
        }