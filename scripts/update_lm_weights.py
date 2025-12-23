#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""CLI-скрипт для ночной оптимизации матрицы весов LM (AOF)."""

import argparse
import asyncio
import json
from typing import Any, Dict

from app.db.manager import DatabaseManager
from app.db.repositories.lm_dictionary_repository import LMDictionaryRepository
from app.services.lm_weight_optimizer import LMWeightOptimizer
from app.logging_config import get_watchdog_logger

logger = get_watchdog_logger(__name__)


async def run_optimizer(days: int, limit: int) -> Dict[str, Any]:
    db_manager = DatabaseManager()
    await db_manager.create_pool()
    try:
        dictionary_repo = LMDictionaryRepository(db_manager)
        optimizer = LMWeightOptimizer(dictionary_repo)
        result = await optimizer.optimize(days=days, sample_limit=limit)
        logger.info("[LM][weights] Optimization result: %s", result)
        return result
    finally:
        await db_manager.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Optimize LM complaint weight matrix")
    parser.add_argument("--days", type=int, default=14, help="Глубина окна в днях")
    parser.add_argument("--limit", type=int, default=20000, help="Максимум хитов для анализа")
    args = parser.parse_args()

    result = asyncio.run(run_optimizer(days=args.days, limit=args.limit))
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
