#!/usr/bin/env python3
"""
Comprehensive Test Script для всех новых сервисов.

Проверяет:
- Импорты всех модулей
- Инициализацию сервисов
- Базовые методы
- Exception handling
- Логирование

Usage:
    python tests/test_new_services.py
"""

import sys
import os
import asyncio
import traceback
from datetime import date, datetime, timedelta

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.db.manager import DatabaseManager
from app.config import DATABASE_CONFIG
from app.logging_config import get_watchdog_logger

logger = get_watchdog_logger(__name__)


class ServiceTester:
    """Comprehensive тестер для всех сервисов."""
    
    def __init__(self):
        self.db_manager = None
        self.test_results = {
            'passed': [],
            'failed': [],
            'errors': []
        }
    
    async def setup(self):
        """Инициализация БД."""
        logger.info("=" * 70)
        logger.info("STARTING COMPREHENSIVE SERVICE TESTING")
        logger.info("=" * 70)
        
        try:
            self.db_manager = DatabaseManager(DATABASE_CONFIG)
            await self.db_manager.initialize()
            logger.info("✅ Database connection established")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to connect to database: {e}")
            logger.error(traceback.format_exc())
            return False
    
    async def teardown(self):
        """Закрытие соединений."""
        if self.db_manager:
            await self.db_manager.close()
            logger.info("✅ Database connection closed")
    
    def log_test(self, test_name: str, passed: bool, error: str = None):
        """Логировать результат теста."""
        if passed:
            self.test_results['passed'].append(test_name)
            logger.info(f"✅ PASSED: {test_name}")
        else:
            self.test_results['failed'].append(test_name)
            logger.error(f"❌ FAILED: {test_name}")
            if error:
                self.test_results['errors'].append({
                    'test': test_name,
                    'error': error
                })
                logger.error(f"   Error: {error}")
    
    # ========================================================================
    # TEST 1: RolesRepository
    # ========================================================================
    
    async def test_roles_repository(self):
        """Тестирование RolesRepository."""
        logger.info("\n" + "=" * 70)
        logger.info("TEST 1: RolesRepository")
        logger.info("=" * 70)
        
        try:
            from app.db.repositories.roles import RolesRepository
            
            repo = RolesRepository(self.db_manager)
            
            # 1.1: Get role by ID
            try:
                role = await repo.get_role_by_id(1)  # Оператор
                assert role is not None, "Role 1 should exist"
                assert role.get('role_name') is not None
                self.log_test("RolesRepository.get_role_by_id", True)
            except Exception as e:
                self.log_test("RolesRepository.get_role_by_id", False, str(e))
            
            # 1.2: Get all roles
            try:
                roles = await repo.get_all_roles()
                assert len(roles) > 0, "Should have at least 1 role"
                self.log_test("RolesRepository.get_all_roles", True)
            except Exception as e:
                self.log_test("RolesRepository.get_all_roles", False, str(e))
            
            # 1.3: Check permission
            try:
                has_perm = await repo.check_permission(7, 'can_manage_users')
                assert isinstance(has_perm, bool)
                self.log_test("RolesRepository.check_permission", True)
            except Exception as e:
                self.log_test("RolesRepository.check_permission", False, str(e))
            
            # 1.4: Get user permissions
            try:
                perms = await repo.get_user_permissions(1)
                assert 'can_view_own_stats' in perms
                self.log_test("RolesRepository.get_user_permissions", True)
            except Exception as e:
                self.log_test("RolesRepository.get_user_permissions", False, str(e))
            
        except ImportError as e:
            self.log_test("RolesRepository IMPORT", False, str(e))
    
    # ========================================================================
    # TEST 2: AdminActionLogger
    # ========================================================================
    
    async def test_admin_action_logger(self):
        """Тестирование AdminActionLogger."""
        logger.info("\n" + "=" * 70)
        logger.info("TEST 2: AdminActionLogger")
        logger.info("=" * 70)
        
        try:
            from app.services.admin_logger import AdminActionLogger
            
            logger_service = AdminActionLogger(self.db_manager)
            
            # 2.1: Log action
            try:
                result = await logger_service.log_action(
                    actor_telegram_id=12345,
                    action='test_action',
                    target_telegram_id=67890,
                    payload={'test': 'data'}
                )
                assert result == True, "Should return True on success"
                self.log_test("AdminActionLogger.log_action", True)
            except Exception as e:
                self.log_test("AdminActionLogger.log_action", False, str(e))
            
            # 2.2: Get recent logs
            try:
                logs = await logger_service.get_recent_logs(limit=10)
                assert isinstance(logs, list)
                self.log_test("AdminActionLogger.get_recent_logs", True)
            except Exception as e:
                self.log_test("AdminActionLogger.get_recent_logs", False, str(e))
            
            # 2.3: Log specific actions
            try:
                await logger_service.log_user_approval(12345, 67890)
                await logger_service.log_role_change(12345, 67890, 'old', 'new')
                self.log_test("AdminActionLogger.specific_methods", True)
            except Exception as e:
                self.log_test("AdminActionLogger.specific_methods", False, str(e))
            
        except ImportError as e:
            self.log_test("AdminActionLogger IMPORT", False, str(e))
    
    # ========================================================================
    # TEST 3: CallAnalyticsRepository
    # ========================================================================
    
    async def test_call_analytics_repository(self):
        """Тестирование CallAnalyticsRepository."""
        logger.info("\n" + "=" * 70)
        logger.info("TEST 3: CallAnalyticsRepository")
        logger.info("=" * 70)
        
        try:
            from app.db.repositories.call_analytics_repo import CallAnalyticsRepository
            
            repo = CallAnalyticsRepository(self.db_manager)
            
            # 3.1: Get call count
            try:
                count = await repo.get_call_count()
                assert isinstance(count, int)
                logger.info(f"   Total calls in call_analytics: {count}")
                self.log_test("CallAnalyticsRepository.get_call_count", True)
            except Exception as e:
                self.log_test("CallAnalyticsRepository.get_call_count", False, str(e))
            
            # 3.2: Get operators list
            try:
                operators = await repo.get_operators_list()
                assert isinstance(operators, list)
                logger.info(f"   Found {len(operators)} operators")
                self.log_test("CallAnalyticsRepository.get_operators_list", True)
            except Exception as e:
                self.log_test("CallAnalyticsRepository.get_operators_list", False, str(e))
            
            # 3.3: Get aggregated metrics (если есть операторы)
            if operators and len(operators) > 0:
                try:
                    test_operator = operators[0]
                    date_from = date.today() - timedelta(days=30)
                    date_to = date.today()
                    
                    metrics = await repo.get_aggregated_metrics(
                        test_operator, date_from, date_to
                    )
                    assert isinstance(metrics, dict)
                    self.log_test("CallAnalyticsRepository.get_aggregated_metrics", True)
                except Exception as e:
                    self.log_test("CallAnalyticsRepository.get_aggregated_metrics", False, str(e))
            
        except ImportError as e:
            self.log_test("CallAnalyticsRepository IMPORT", False, str(e))
    
    # ========================================================================
    # TEST 4: CallAnalyticsSyncService
    # ========================================================================
    
    async def test_call_analytics_sync(self):
        """Тестирование CallAnalyticsSyncService."""
        logger.info("\n" + "=" * 70)
        logger.info("TEST 4: CallAnalyticsSyncService")
        logger.info("=" * 70)
        
        try:
            from app.services.call_analytics_sync import CallAnalyticsSyncService
            
            sync_service = CallAnalyticsSyncService(self.db_manager)
            
            # 4.1: Get sync status
            try:
                status = await sync_service.get_sync_status()
                assert 'call_scores_count' in status
                assert 'call_analytics_count' in status
                logger.info(f"   Sync status: {status.get('sync_percentage', 0):.1f}%")
                self.log_test("CallAnalyticsSyncService.get_sync_status", True)
            except Exception as e:
                self.log_test("CallAnalyticsSyncService.get_sync_status", False, str(e))
            
            # 4.2: Test incremental sync (small batch)
            try:
                stats = await sync_service.sync_new(batch_size=10)
                assert 'inserted' in stats
                logger.info(f"   Incremental sync: {stats.get('inserted', 0)} inserted")
                self.log_test("CallAnalyticsSyncService.sync_new", True)
            except Exception as e:
                self.log_test("CallAnalyticsSyncService.sync_new", False, str(e))
            
        except ImportError as e:
            self.log_test("CallAnalyticsSyncService IMPORT", False, str(e))
    
    # ========================================================================
    # TEST 5: DashboardCacheService
    # ========================================================================
    
    async def test_dashboard_cache(self):
        """Тестирование DashboardCacheService."""
        logger.info("\n" + "=" * 70)
        logger.info("TEST 5: DashboardCacheService")
        logger.info("=" * 70)
        
        try:
            from app.services.dashboard_cache import DashboardCacheService
            
            cache_service = DashboardCacheService(self.db_manager)
            
            # 5.1: Save dashboard
            try:
                test_metrics = {
                    'total_calls': 100,
                    'records': 50,
                    'conversion_rate': 50.0,
                    'avg_score_all': 8.5
                }
                
                result = await cache_service.save_dashboard_cache(
                    operator_name='TestOperator',
                    period_type='day',
                    period_start=date.today(),
                    period_end=date.today(),
                    metrics=test_metrics
                )
                assert result == True
                self.log_test("DashboardCacheService.save_dashboard_cache", True)
            except Exception as e:
                self.log_test("DashboardCacheService.save_dashboard_cache", False, str(e))
            
            # 5.2: Get cached dashboard
            try:
                cached = await cache_service.get_cached_dashboard(
                    operator_name='TestOperator',
                    period_type='day',
                    period_start=date.today()
                )
                # Может быть None если TTL истёк
                self.log_test("DashboardCacheService.get_cached_dashboard", True)
            except Exception as e:
                self.log_test("DashboardCacheService.get_cached_dashboard", False, str(e))
            
            # 5.3: Invalidate cache
            try:
                result = await cache_service.invalidate_cache('TestOperator')
                assert result == True
                self.log_test("DashboardCacheService.invalidate_cache", True)
            except Exception as e:
                self.log_test("DashboardCacheService.invalidate_cache", False, str(e))
            
        except ImportError as e:
            self.log_test("DashboardCacheService IMPORT", False, str(e))
    
    # ========================================================================
    # TEST 6: Updated Analytics Repository
    # ========================================================================
    
    async def test_analytics_repository(self):
        """Тестирование обновленного AnalyticsRepository."""
        logger.info("\n" + "=" * 70)
        logger.info("TEST 6: AnalyticsRepository (Updated)")
        logger.info("=" * 70)
        
        try:
            from app.db.repositories.analytics import AnalyticsRepository
            
            repo = AnalyticsRepository(self.db_manager)
            
            # 6.1: Check CallAnalyticsRepository integration
            try:
                assert hasattr(repo, 'call_analytics_repo')
                self.log_test("AnalyticsRepository.call_analytics_integration", True)
            except Exception as e:
                self.log_test("AnalyticsRepository.call_analytics_integration", False, str(e))
            
            # 6.2: Save recommendations
            try:
                result = await repo.save_operator_recommendations(
                    operator_name='TestOperator',
                    report_date=date.today(),
                    recommendations='Test recommendations',
                    call_samples_analyzed=10
                )
                assert result == True
                self.log_test("AnalyticsRepository.save_operator_recommendations", True)
            except Exception as e:
                self.log_test("AnalyticsRepository.save_operator_recommendations", False, str(e))
            
            # 6.3: Get recommendations
            try:
                recs = await repo.get_operator_recommendations(
                    operator_name='TestOperator',
                    report_date=date.today()
                )
                self.log_test("AnalyticsRepository.get_operator_recommendations", True)
            except Exception as e:
                self.log_test("AnalyticsRepository.get_operator_recommendations", False, str(e))
            
        except ImportError as e:
            self.log_test("AnalyticsRepository IMPORT", False, str(e))
    
    # ========================================================================
    # TEST 7: Handlers Import Check
    # ========================================================================
    
    async def test_handlers_import(self):
        """Проверка импортов всех handlers."""
        logger.info("\n" + "=" * 70)
        logger.info("TEST 7: Handlers Import Check")
        logger.info("=" * 70)
        
        handlers_to_test = [
            ('sync_analytics', 'app.telegram.handlers.sync_analytics', 'SyncAnalyticsHandler'),
            ('start', 'app.telegram.handlers.start', 'StartHandler'),
            ('help', 'app.telegram.handlers.help', 'HelpHandler'),
        ]
        
        for name, module_path, class_name in handlers_to_test:
            try:
                module = __import__(module_path, fromlist=[class_name])
                handler_class = getattr(module, class_name)
                # Попытка создать экземпляр
                instance = handler_class(self.db_manager)
                self.log_test(f"Handler.{name}_import", True)
            except Exception as e:
                self.log_test(f"Handler.{name}_import", False, str(e))
    
    # ========================================================================
    # TEST 8: KeyboardBuilder
    # ========================================================================
    
    async def test_keyboard_builder(self):
        """Тестирование KeyboardBuilder."""
        logger.info("\n" + "=" * 70)
        logger.info("TEST 8: KeyboardBuilder")
        logger.info("=" * 70)
        
        try:
            from app.telegram.utils.keyboard_builder import KeyboardBuilder
            from app.db.repositories.roles import RolesRepository
            
            roles_repo = RolesRepository(self.db_manager)
            builder = KeyboardBuilder(roles_repo)
            
            # 8.1: Build main keyboard
            try:
                keyboard = await builder.build_main_keyboard(role_id=1)
                assert keyboard is not None
                self.log_test("KeyboardBuilder.build_main_keyboard", True)
            except Exception as e:
                self.log_test("KeyboardBuilder.build_main_keyboard", False, str(e))
            
            # 8.2: Build reports menu
            try:
                menu = builder.build_reports_menu(can_view_all=True)
                assert menu is not None
                self.log_test("KeyboardBuilder.build_reports_menu", True)
            except Exception as e:
                self.log_test("KeyboardBuilder.build_reports_menu", False, str(e))
            
            # 8.3: Build other menus
            try:
                builder.build_call_lookup_menu()
                builder.build_users_management_menu()
                builder.build_system_menu()
                self.log_test("KeyboardBuilder.other_menus", True)
            except Exception as e:
                self.log_test("KeyboardBuilder.other_menus", False, str(e))
            
        except ImportError as e:
            self.log_test("KeyboardBuilder IMPORT", False, str(e))
    
    # ========================================================================
    # MAIN TEST RUNNER
    # ========================================================================
    
    async def run_all_tests(self):
        """Запустить все тесты."""
        if not await self.setup():
            logger.error("Failed to setup, aborting tests")
            return
        
        try:
            await self.test_roles_repository()
            await self.test_admin_action_logger()
            await self.test_call_analytics_repository()
            await self.test_call_analytics_sync()
            await self.test_dashboard_cache()
            await self.test_analytics_repository()
            await self.test_handlers_import()
            await self.test_keyboard_builder()
            
        finally:
            await self.teardown()
        
        # Print summary
        self.print_summary()
    
    def print_summary(self):
        """Вывести итоговый отчет."""
        logger.info("\n" + "=" * 70)
        logger.info("TEST SUMMARY")
        logger.info("=" * 70)
        
        total = len(self.test_results['passed']) + len(self.test_results['failed'])
        passed = len(self.test_results['passed'])
        failed = len(self.test_results['failed'])
        
        logger.info(f"Total tests: {total}")
        logger.info(f"✅ Passed: {passed}")
        logger.info(f"❌ Failed: {failed}")
        
        if failed > 0:
            logger.info("\n❌ FAILED TESTS:")
            for test in self.test_results['failed']:
                logger.info(f"   - {test}")
            
            if self.test_results['errors']:
                logger.info("\n📋 ERROR DETAILS:")
                for error in self.test_results['errors']:
                    logger.info(f"   {error['test']}: {error['error']}")
        
        logger.info("\n" + "=" * 70)
        if failed == 0:
            logger.info("🎉 ALL TESTS PASSED!")
        else:
            logger.info(f"⚠️  {failed} TEST(S) FAILED")
        logger.info("=" * 70)


async def main():
    """Main entry point."""
    tester = ServiceTester()
    await tester.run_all_tests()


if __name__ == '__main__':
    asyncio.run(main())
