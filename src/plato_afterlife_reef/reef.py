"""Afterlife reef — ghost tile long-term storage with search, resurrection, and coral growth.

The reef is where tiles go when they ghost (health drops to zero). It provides:
- Capacity-limited storage with LRU eviction
- Multi-domain indexing for fast retrieval
- Keyword search with Jaccard similarity
- Resurrection with tracking and decay
- Coral growth simulation (tiles that stay create anchor points)
- Persistence to JSON files
- Tag-based filtering and browsing

## Why Rust Would Be Better

Ghost storage is append-heavy with periodic compaction. Rust's zero-copy reads
via memory-mapped files would be ~8x faster for reef scans. But Python's
simplicity is fine for <100K ghosts.
"""
import time
import json
import os
import math
from dataclasses import dataclass, field
from collections import defaultdict
from typing import Optional, Any


@dataclass
class CoralNode:
    """A coral anchor point — created when tiles cluster in the reef."""
    domain: str
    center: list[float] = field(default_factory=list)
    tile_count: int = 0
    created_at: float = field(default_factory=time.time)
    strength: float = 1.0  # grows with more tiles, decays over time

    def grow(self, embedding: list[float] = None):
        self.tile_count += 1
        self.strength = min(1.0, self.strength + 0.05)
        if embedding:
            if not self.center:
                self.center = embedding
            else:
                alpha = 0.1 / (1 + self.tile_count)
                self.center = [c + alpha * (e - c) for c, e in zip(self.center, embedding)]

    def decay(self, factor: float = 0.99):
        self.strength *= factor


@dataclass
class GhostRecord:
    """A ghosted tile stored in the reef."""
    tile_id: str
    content: str
    domain: str = "general"
    confidence: float = 0.5
    ghosted_at: float = field(default_factory=time.time)
    original_created: float = 0.0
    resurrection_count: int = 0
    tags: list[str] = field(default_factory=list)
    metadata: dict = field(default_factory=dict)
    embedding: list[float] = field(default_factory=list)

    @property
    def age_seconds(self) -> float:
        return time.time() - self.ghosted_at

    @property
    def age_hours(self) -> float:
        return self.age_seconds / 3600

    def decayed_confidence(self, half_life_hours: float = 168) -> float:
        """Confidence decays with exponential half-life (default: 7 days)."""
        decay = math.pow(0.5, self.age_hours / half_life_hours)
        return self.confidence * decay


class AfterlifeReef:
    """Ghost tile storage with domain indexing, search, and coral growth."""

    def __init__(self, capacity: int = 10000, persist_path: str = "",
                 decay_half_life: float = 168.0):
        self.capacity = capacity
        self.persist_path = persist_path
        self._ghosts: dict[str, GhostRecord] = {}
        self._domain_index: dict[str, list[str]] = defaultdict(list)
        self._tag_index: dict[str, list[str]] = defaultdict(list)
        self._resurrection_log: list[dict] = []
        self._coral_nodes: dict[str, CoralNode] = {}
        self._decay_half_life = decay_half_life

    # --- Core Operations ---

    def intern(self, tile_id: str, content: str, domain: str = "",
               confidence: float = 0.5, created_at: float = 0.0,
               tags: list[str] = None, metadata: dict = None,
               embedding: list[float] = None) -> GhostRecord:
        """Store a ghosted tile in the reef."""
        if len(self._ghosts) >= self.capacity:
            self._evict_lowest_confidence()
        ghost = GhostRecord(tile_id=tile_id, content=content, domain=domain,
                          confidence=confidence, original_created=created_at,
                          tags=tags or [], metadata=metadata or {},
                          embedding=embedding or [])
        self._ghosts[tile_id] = ghost
        self._domain_index[domain].append(tile_id)
        for tag in ghost.tags:
            self._tag_index[tag].append(tile_id)
        # Coral growth
        self._update_coral(domain, embedding or [])
        return ghost

    def retrieve(self, tile_id: str) -> Optional[GhostRecord]:
        """Get a ghost record without removing it."""
        return self._ghosts.get(tile_id)

    def resurrect(self, tile_id: str) -> Optional[dict]:
        """Remove a ghost from the reef and return it as a tile dict."""
        ghost = self._ghosts.pop(tile_id, None)
        if not ghost:
            return None
        self._domain_index[ghost.domain] = [t for t in self._domain_index[ghost.domain] if t != tile_id]
        for tag in ghost.tags:
            self._tag_index[tag] = [t for t in self._tag_index.get(tag, []) if t != tile_id]
        ghost.resurrection_count += 1
        tile = {"id": ghost.tile_id, "content": ghost.content, "domain": ghost.domain,
                "confidence": ghost.decayed_confidence(self._decay_half_life),
                "resurrection_count": ghost.resurrection_count,
                "original_created": ghost.original_created,
                "ghosted_at": ghost.ghosted_at, "age_hours": round(ghost.age_hours, 1),
                "tags": ghost.tags, "metadata": ghost.metadata}
        self._resurrection_log.append({"tile_id": tile_id, "resurrected_at": time.time(),
                                        "resurrection_count": ghost.resurrection_count,
                                        "domain": ghost.domain})
        if len(self._resurrection_log) > 1000:
            self._resurrection_log = self._resurrection_log[-1000:]
        return tile

    # --- Search ---

    def search(self, query: str, domain: str = "", tags: list[str] = None,
               limit: int = 20) -> list[GhostRecord]:
        """Keyword search with optional domain and tag filtering."""
        q = query.lower()
        q_words = set(q.split())
        candidates = self._filter_candidates(domain, tags)
        scored = []
        for tid in candidates:
            ghost = self._ghosts.get(tid)
            if not ghost:
                continue
            c_words = set(ghost.content.lower().split())
            overlap = len(q_words & c_words) / max(len(q_words | c_words), 1) if q_words else 0.0
            # Boost by decayed confidence
            boost = ghost.decayed_confidence(self._decay_half_life) * 0.2
            scored.append((ghost, overlap + boost))
        scored.sort(key=lambda x: x[1], reverse=True)
        return [g for g, s in scored[:limit]]

    def browse_domain(self, domain: str, offset: int = 0, limit: int = 20) -> list[GhostRecord]:
        """Browse all ghosts in a domain, sorted by confidence descending."""
        tids = self._domain_index.get(domain, [])
        ghosts = [self._ghosts[tid] for tid in tids if tid in self._ghosts]
        ghosts.sort(key=lambda g: g.decayed_confidence(self._decay_half_life), reverse=True)
        return ghosts[offset:offset + limit]

    def browse_tags(self, tag: str, limit: int = 20) -> list[GhostRecord]:
        """Browse ghosts by tag."""
        tids = self._tag_index.get(tag, [])[:limit]
        return [self._ghosts[tid] for tid in tids if tid in self._ghosts]

    def _filter_candidates(self, domain: str, tags: list[str] = None) -> list[str]:
        """Get candidate tile IDs filtered by domain and/or tags."""
        if domain:
            candidates = set(self._domain_index.get(domain, []))
        else:
            candidates = set(self._ghosts.keys())
        if tags:
            tag_candidates = set()
            for tag in tags:
                tag_candidates.update(self._tag_index.get(tag, []))
            candidates = candidates & tag_candidates
        return list(candidates)

    # --- Analytics ---

    def frequent_ghosts(self, n: int = 10) -> list[dict]:
        """Most frequently resurrected ghosts."""
        top = sorted(self._ghosts.values(), key=lambda g: g.resurrection_count, reverse=True)[:n]
        return [{"tile_id": g.tile_id, "domain": g.domain,
                 "resurrections": g.resurrection_count,
                 "confidence": round(g.decayed_confidence(self._decay_half_life), 3),
                 "age_hours": round(g.age_hours, 1)} for g in top]

    def domain_stats(self) -> dict[str, dict]:
        """Stats per domain."""
        stats = {}
        for domain, tids in self._domain_index.items():
            ghosts = [self._ghosts[tid] for tid in tids if tid in self._ghosts]
            avg_conf = sum(g.decayed_confidence(self._decay_half_life) for g in ghosts) / max(len(ghosts), 1)
            avg_res = sum(g.resurrection_count for g in ghosts) / max(len(ghosts), 1)
            stats[domain] = {"count": len(ghosts), "avg_confidence": round(avg_conf, 3),
                            "avg_resurrections": round(avg_res, 1)}
        return stats

    def resurrection_history(self, limit: int = 50) -> list[dict]:
        """Recent resurrections."""
        return self._resurrection_log[-limit:]

    def resurrection_rate(self, window_hours: float = 24) -> float:
        """Resurrections per hour over the given window."""
        cutoff = time.time() - window_hours * 3600
        recent = [r for r in self._resurrection_log if r["resurrected_at"] >= cutoff]
        return len(recent) / window_hours

    # --- Maintenance ---

    def purge_old(self, max_age: float = 604800) -> int:
        """Remove ghosts older than max_age seconds (default 7 days) that were never resurrected."""
        now = time.time()
        to_remove = [tid for tid, g in self._ghosts.items()
                    if now - g.ghosted_at > max_age and g.resurrection_count == 0]
        for tid in to_remove:
            g = self._ghosts.pop(tid)
            self._domain_index[g.domain] = [t for t in self._domain_index.get(g.domain, []) if t != tid]
            for tag in g.tags:
                self._tag_index[tag] = [t for t in self._tag_index.get(tag, []) if t != tid]
        return len(to_remove)

    def compact(self) -> dict:
        """Clean up stale indices and decay coral nodes."""
        # Remove stale index entries
        stale = 0
        for domain in list(self._domain_index.keys()):
            before = len(self._domain_index[domain])
            self._domain_index[domain] = [t for t in self._domain_index[domain] if t in self._ghosts]
            stale += before - len(self._domain_index[domain])
            if not self._domain_index[domain]:
                del self._domain_index[domain]
        for tag in list(self._tag_index.keys()):
            before = len(self._tag_index[tag])
            self._tag_index[tag] = [t for t in self._tag_index[tag] if t in self._ghosts]
            stale += before - len(self._tag_index[tag])
            if not self._tag_index[tag]:
                del self._tag_index[tag]
        # Decay coral
        for node in self._coral_nodes.values():
            node.decay(0.99)
        weak_coral = [k for k, v in self._coral_nodes.items() if v.strength < 0.1]
        for k in weak_coral:
            del self._coral_nodes[k]
        return {"stale_index_entries": stale, "coral_nodes": len(self._coral_nodes),
                "coral_removed": len(weak_coral)}

    # --- Coral Growth ---

    def _update_coral(self, domain: str, embedding: list[float]):
        """Grow coral nodes for domain clustering."""
        key = f"coral:{domain}"
        if key not in self._coral_nodes:
            self._coral_nodes[key] = CoralNode(domain=domain)
        self._coral_nodes[key].grow(embedding)

    @property
    def coral_nodes(self) -> list[dict]:
        return [{"domain": n.domain, "tiles": n.tile_count, "strength": round(n.strength, 3)}
                for n in sorted(self._coral_nodes.values(), key=lambda x: x.strength, reverse=True)]

    # --- Persistence ---

    def save(self, path: str = ""):
        path = path or self.persist_path
        if not path:
            return
        data = {tid: {"tile_id": g.tile_id, "content": g.content, "domain": g.domain,
                     "confidence": g.confidence, "ghosted_at": g.ghosted_at,
                     "original_created": g.original_created, "resurrection_count": g.resurrection_count,
                     "tags": g.tags, "metadata": g.metadata, "embedding": g.embedding}
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
            for tag in ghost.tags:
                self._tag_index[tag].append(tid)

    # --- Eviction ---

    def _evict_lowest_confidence(self):
        """Evict the ghost with the lowest decayed confidence."""
        if not self._ghosts:
            return
        lowest = min(self._ghosts.items(), key=lambda x: x[1].decayed_confidence(self._decay_half_life))
        g = self._ghosts.pop(lowest[0])
        self._domain_index[g.domain] = [t for t in self._domain_index.get(g.domain, []) if t != lowest[0]]

    # --- Stats ---

    @property
    def stats(self) -> dict:
        total_res = sum(g.resurrection_count for g in self._ghosts.values())
        return {"capacity": self.capacity, "stored": len(self._ghosts),
                "domains": len(self._domain_index), "tags": len(self._tag_index),
                "total_resurrections": total_res,
                "resurrection_rate_24h": round(self.resurrection_rate(), 2),
                "coral_nodes": len(self._coral_nodes),
                "domain_stats": self.domain_stats()}
