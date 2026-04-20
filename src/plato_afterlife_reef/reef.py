"""Ghost tile long-term storage — the reef where tiles go when they ghost."""
import time
import json
import os
from dataclasses import dataclass, field
from collections import defaultdict
from typing import Optional

@dataclass
class GhostRecord:
    tile_id: str
    content: str
    domain: str = "general"
    confidence: float = 0.5
    ghosted_at: float = field(default_factory=time.time)
    original_created: float = 0.0
    resurrection_count: int = 0
    tags: list[str] = field(default_factory=list)
    metadata: dict = field(default_factory=dict)

class AfterlifeReef:
    def __init__(self, capacity: int = 10000, persist_path: str = ""):
        self.capacity = capacity
        self.persist_path = persist_path
        self._ghosts: dict[str, GhostRecord] = {}
        self._domain_index: dict[str, list[str]] = defaultdict(list)
        self._resurrection_log: list[dict] = []

    def intern(self, tile_id: str, content: str, domain: str = "",
               confidence: float = 0.5, created_at: float = 0.0,
               tags: list[str] = None) -> GhostRecord:
        if len(self._ghosts) >= self.capacity:
            self._evict_oldest()
        ghost = GhostRecord(tile_id=tile_id, content=content, domain=domain,
                          confidence=confidence, original_created=created_at,
                          tags=tags or [])
        self._ghosts[tile_id] = ghost
        self._domain_index[domain].append(tile_id)
        return ghost

    def retrieve(self, tile_id: str) -> Optional[GhostRecord]:
        return self._ghosts.get(tile_id)

    def resurrect(self, tile_id: str) -> Optional[dict]:
        ghost = self._ghosts.pop(tile_id, None)
        if not ghost:
            return None
        self._domain_index[ghost.domain] = [t for t in self._domain_index[ghost.domain] if t != tile_id]
        ghost.resurrection_count += 1
        tile = {"id": ghost.tile_id, "content": ghost.content, "domain": ghost.domain,
                "confidence": ghost.confidence, "resurrection_count": ghost.resurrection_count,
                "original_created": ghost.original_created,
                "tags": ghost.tags, "metadata": ghost.metadata}
        self._resurrection_log.append({"tile_id": tile_id, "resurrected_at": time.time(),
                                        "resurrection_count": ghost.resurrection_count})
        if len(self._resurrection_log) > 1000:
            self._resurrection_log = self._resurrection_log[-1000:]
        return tile

    def search(self, query: str, domain: str = "", limit: int = 20) -> list[GhostRecord]:
        q = query.lower()
        candidates = self._domain_index.get(domain, list(self._ghosts.keys())) if domain else list(self._ghosts.keys())
        scored = []
        for tid in candidates:
            ghost = self._ghosts.get(tid)
            if not ghost:
                continue
            q_words = set(q.split())
            c_words = set(ghost.content.lower().split())
            overlap = len(q_words & c_words) / max(len(q_words | c_words), 1) if q_words else 0.0
            scored.append((ghost, overlap))
        scored.sort(key=lambda x: x[1], reverse=True)
        return [g for g, s in scored[:limit]]

    def frequent_ghosts(self, n: int = 10) -> list[GhostRecord]:
        return sorted(self._ghosts.values(), key=lambda g: g.resurrection_count, reverse=True)[:n]

    def purge_old(self, max_age: float = 604800) -> int:  # 7 days default
        now = time.time()
        to_remove = [tid for tid, g in self._ghosts.items()
                    if now - g.ghosted_at > max_age and g.resurrection_count == 0]
        for tid in to_remove:
            g = self._ghosts.pop(tid)
            self._domain_index[g.domain] = [t for t in self._domain_index.get(g.domain, []) if t != tid]
        return len(to_remove)

    def _evict_oldest(self):
        oldest = min(self._ghosts.items(), key=lambda x: x[1].ghosted_at)
        g = self._ghosts.pop(oldest[0])
        self._domain_index[g.domain] = [t for t in self._domain_index.get(g.domain, []) if t != oldest[0]]

    def save(self, path: str = ""):
        path = path or self.persist_path
        if not path:
            return
        data = {tid: {"tile_id": g.tile_id, "content": g.content, "domain": g.domain,
                     "confidence": g.confidence, "ghosted_at": g.ghosted_at,
                     "original_created": g.original_created, "resurrection_count": g.resurrection_count,
                     "tags": g.tags, "metadata": g.metadata}
                for tid, g in self._ghosts.items()}
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(path, "w") as f:
            json.dump(data, f, indent=2)

    def load(self, path: str = ""):
        path = path or self.persist_path
        if not path or not os.path.exists(path):
            return
        with open(path) as f:
            data = json.load(f)
        for tid, d in data.items():
            ghost = GhostRecord(**d)
            self._ghosts[tid] = ghost
            self._domain_index[ghost.domain].append(tid)

    @property
    def stats(self) -> dict:
        domains = {d: len(tiles) for d, tiles in self._domain_index.items() if tiles}
        total_resurrections = sum(g.resurrection_count for g in self._ghosts.values())
        return {"capacity": self.capacity, "stored": len(self._ghosts),
                "domains": domains, "total_resurrections": total_resurrections,
                "domain_index_size": sum(len(v) for v in self._domain_index.values())}
