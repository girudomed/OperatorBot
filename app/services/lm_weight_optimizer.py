# -*- coding: utf-8 -*-
"""AOF job: автоматическая корректировка весов словаря жалоб."""

from __future__ import annotations

import math
from collections import Counter
from typing import Dict, List, Any, Optional

from app.services.lm_weights import ComplaintWeightMatrix
from app.db.repositories.lm_dictionary_repository import LMDictionaryRepository
from app.logging_config import get_watchdog_logger
from app.services.lm_service import (
    COMPLAINT_LEGAL_KEYWORDS,
    COMPLAINT_BEHAVIOR_KEYWORDS,
    COMPLAINT_PROCESS_KEYWORDS,
    COMPLAINT_REFUND_KEYWORDS,
    FOLLOWUP_SPAM_KEYWORDS,
    FOLLOWUP_AUTO_RESPONSES,
    INFO_REQUEST_KEYWORDS,
)

logger = get_watchdog_logger(__name__)


def _contains(haystack_parts: List[str], keywords: List[str]) -> bool:
    lowered = [part.lower() for part in haystack_parts if part]
    for text in lowered:
        for kw in keywords:
            if kw and kw.lower() in text:
                return True
    return False


def classify_hit_category(term: Optional[str], snippet: Optional[str]) -> Optional[str]:
    haystacks = [term or "", snippet or ""]
    if _contains(haystacks, COMPLAINT_LEGAL_KEYWORDS):
        return "legal"
    if _contains(haystacks, COMPLAINT_REFUND_KEYWORDS):
        return "refund"
    if _contains(haystacks, COMPLAINT_BEHAVIOR_KEYWORDS):
        return "behavior"
    if _contains(haystacks, COMPLAINT_PROCESS_KEYWORDS):
        return "process"
    if _contains(haystacks, INFO_REQUEST_KEYWORDS):
        return "info_request"
    spam_keywords = FOLLOWUP_SPAM_KEYWORDS + FOLLOWUP_AUTO_RESPONSES
    if _contains(haystacks, spam_keywords):
        return "spam"
    return None


class LMWeightOptimizer:
    """Собирает статистику по словарным срабатываниям и обновляет матрицу весов."""

    def __init__(
        self,
        dictionary_repo: LMDictionaryRepository,
        matrix: Optional[ComplaintWeightMatrix] = None,
    ) -> None:
        self.dictionary_repo = dictionary_repo
        self.matrix = matrix or ComplaintWeightMatrix()

    async def optimize(
        self,
        *,
        dict_code: str = "complaint_risk",
        days: int = 14,
        sample_limit: int = 20000,
    ) -> Dict[str, Any]:
        hits = await self.dictionary_repo.get_recent_hits(
            dict_code,
            days=days,
            limit=sample_limit,
        )
        if not hits:
            logger.info("[LM][weights] Недостаточно данных для оптимизации матрицы (%s дней)", days)
            return {"updated": False, "reason": "no_data"}

        stats = self._aggregate_hits(hits)
        self._adjust_threshold(stats)
        self._adjust_categories(stats)
        self.matrix.save()
        return {
            "updated": True,
            "stats": stats,
            "threshold": self.matrix.thresholds.get("complaint_score"),
        }

    def _aggregate_hits(self, hits: List[Dict[str, Any]]) -> Dict[str, Any]:
        category_counts: Counter[str] = Counter()
        weighted_impact: Counter[str] = Counter()
        total_hits = 0
        total_weight = 0.0

        for hit in hits:
            total_hits += 1
            cat = classify_hit_category(hit.get("term"), hit.get("snippet")) or "other"
            weight = float(hit.get("weight") or 0.0)
            hit_count = int(hit.get("hit_count") or 1)
            category_counts[cat] += hit_count
            weighted_impact[cat] += weight * hit_count
            total_weight += weight * hit_count

        return {
            "total_hits": total_hits,
            "category_hits": dict(category_counts),
            "category_weight": dict(weighted_impact),
            "total_weight": total_weight,
        }

    def _adjust_threshold(self, stats: Dict[str, Any]) -> None:
        total_hits = max(1, int(stats.get("total_hits") or 1))
        category_hits = stats.get("category_hits", {})
        noise_hits = category_hits.get("info_request", 0) + category_hits.get("spam", 0)
        noise_ratio = min(0.8, noise_hits / total_hits)

        base_threshold = 55.0
        adjustment = noise_ratio * 25.0  # больше шума — выше требуемый скор
        new_threshold = round(base_threshold + adjustment, 1)
        self.matrix.set_threshold("complaint_score", new_threshold)
        logger.info(
            "[LM][weights] complaint_score threshold → %s (noise_ratio=%.2f)",
            new_threshold,
            noise_ratio,
        )

    def _adjust_categories(self, stats: Dict[str, Any]) -> None:
        category_hits = stats.get("category_hits", {})
        total_hits = max(1, int(stats.get("total_hits") or 1))

        def ratio(label: str) -> float:
            return min(1.0, category_hits.get(label, 0) / total_hits)

        # Чем больше шума, тем сильнее штрафуем категории info_request и spam
        info_multiplier = -1.0 - ratio("info_request") * 2.0
        spam_multiplier = -2.0 - ratio("spam") * 2.0
        self.matrix.set_category_params("info_request", multiplier=info_multiplier, bias=-5.0)
        self.matrix.set_category_params("spam", multiplier=spam_multiplier, bias=-10.0)

        # Поддержим полезные категории: если они часто встречаются, слегка усиливаем
        for label in ("legal", "refund", "behavior"):
            share = ratio(label)
            boost = 1.0 + min(0.5, share)
            current_bias = math.log1p(max(0.0, share)) * 5.0
            self.matrix.set_category_params(label, multiplier=boost, bias=current_bias)


__all__ = ["LMWeightOptimizer", "classify_hit_category"]
