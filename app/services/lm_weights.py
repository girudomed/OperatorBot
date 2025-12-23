# -*- coding: utf-8 -*-
"""Управление матрицей весов для классификации жалоб."""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Any, Optional
import json
import copy


DEFAULT_MATRIX: Dict[str, Any] = {
    "thresholds": {
        "complaint_score": 60.0,
    },
    "categories": {
        "legal": {"multiplier": 1.3},
        "behavior": {"multiplier": 1.2},
        "process": {"multiplier": 1.1},
        "refund": {"multiplier": 1.4},
        "info_request": {"multiplier": 0.0},
        "spam": {"multiplier": -2.0},
    },
}


class ComplaintWeightMatrix:
    """Читает/хранит локальную матрицу весов для категорий жалоб."""

    def __init__(self, path: Optional[str] = None):
        self.path = Path(path or "config/lm_weight_matrix.json")
        self._config: Dict[str, Any] = copy.deepcopy(DEFAULT_MATRIX)
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            return
        try:
            with self.path.open("r", encoding="utf-8") as fp:
                data = json.load(fp)
            self._deep_update(self._config, data)
        except Exception:
            # В случае ошибки используем дефолты.
            return

    def save(self) -> None:
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with self.path.open("w", encoding="utf-8") as fp:
                json.dump(self._config, fp, ensure_ascii=False, indent=2)
        except Exception:
            pass

    @property
    def thresholds(self) -> Dict[str, float]:
        return self._config.get("thresholds", {})

    def resolve_threshold(self, key: str, fallback: float) -> float:
        try:
            return float(self.thresholds.get(key, fallback))
        except (TypeError, ValueError):
            return fallback

    def set_threshold(self, key: str, value: float) -> None:
        self._config.setdefault("thresholds", {})[key] = float(value)

    def set_category_params(
        self,
        category: str,
        *,
        multiplier: Optional[float] = None,
        bias: Optional[float] = None,
    ) -> None:
        bucket = self._config.setdefault("categories", {}).setdefault(category, {})
        if multiplier is not None:
            bucket["multiplier"] = float(multiplier)
        if bias is not None:
            bucket["bias"] = float(bias)

    def apply_multiplier(self, category: Optional[str], base_value: float) -> float:
        if not category:
            return base_value
        cat_conf = self._config.get("categories", {}).get(category)
        if not cat_conf:
            return base_value
        multiplier = self._safe_float(cat_conf.get("multiplier"), 1.0)
        bias = self._safe_float(cat_conf.get("bias"), 0.0)
        return base_value * multiplier + bias

    @staticmethod
    def _safe_float(value: Any, fallback: float) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return fallback

    @staticmethod
    def _deep_update(target: Dict[str, Any], incoming: Dict[str, Any]) -> None:
        for key, value in incoming.items():
            if isinstance(value, dict) and isinstance(target.get(key), dict):
                ComplaintWeightMatrix._deep_update(target[key], value)
            else:
                target[key] = value


__all__ = ["ComplaintWeightMatrix", "DEFAULT_MATRIX"]
