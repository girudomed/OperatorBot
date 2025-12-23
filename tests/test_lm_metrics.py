"""
Тесты для проверки математики LM метрик.
"""

import unittest
from app.services.lm_service import LMService
from app.db.repositories.lm_repository import LMRepository


class TestLMMetricsCalculations(unittest.TestCase):
    """Тесты математики LM метрик."""
    
    def setUp(self):
        """Подготовка тестовых данных."""
        # Mock repository
        self.lm_service = LMService(lm_repository=None)  # type: ignore
        
        # Тестовые данные
        self.test_call_history = {
            'id': 1,
            'call_date': None,
            'talk_duration': 120,  # 2 минуты
            'call_type': 'входящий',
            'caller_number': '+79991234567'
        }
        
        self.test_call_score = {
            'id': 1,
            'call_score': 8.5,
            'outcome': 'record',
            'call_category': 'Запись на услугу (успешная)',
            'is_target': 1,
            'refusal_reason': None,
            'requested_service_name': 'Консультация',
            'number_checklist': 8
        }
    
    # =================================================================
    # OPERATIONAL METRICS TESTS
    # =================================================================
    
    def test_response_speed_answered_call(self):
        """Тест: принятый звонок должен иметь высокий response_speed."""
        speed_value, speed_label = self.lm_service._calculate_response_speed(
            self.test_call_history,
            self.test_call_score
        )
        self.assertEqual(speed_value, 5.0)
        self.assertEqual(speed_label, 'green')
        self.assertGreaterEqual(speed_value, 0)
        self.assertLessEqual(speed_value, 5)
    
    def test_response_speed_missed_call(self):
        """Тест: пропущенный звонок должен иметь низкий response_speed."""
        missed_call = self.test_call_history.copy()
        missed_call['talk_duration'] = 0
        
        speed_value, speed_label = self.lm_service._calculate_response_speed(
            missed_call,
            None
        )
        self.assertEqual(speed_value, 1.0)
        self.assertEqual(speed_label, 'red')
    
    def test_talk_efficiency_long_call(self):
        """Тест: длинный разговор должен иметь высокую эффективность."""
        efficiency = self.lm_service._calculate_talk_efficiency(
            self.test_call_history,
            self.test_call_score
        )
        # 120 сек / 3 = 40
        self.assertEqual(efficiency, 40.0)
        self.assertLessEqual(efficiency, 100.0)
    
    def test_talk_efficiency_short_call(self):
        """Тест: короткий разговор должен иметь низкую эффективность."""
        short_call = self.test_call_history.copy()
        short_call['talk_duration'] = 15  # 15 секунд
        
        efficiency = self.lm_service._calculate_talk_efficiency(
            short_call,
            self.test_call_score
        )
        # 15 * 2 = 30
        self.assertEqual(efficiency, 30.0)
    
    def test_queue_impact(self):
        """Тест: расчёт влияния на очередь."""
        impact = self.lm_service._calculate_queue_impact(
            self.test_call_history,
            self.test_call_score
        )
        # 120 / 300 * 100 = 40
        self.assertEqual(impact, 40.0)
        self.assertLessEqual(impact, 100.0)
    
    # =================================================================
    # CONVERSION METRICS TESTS
    # =================================================================
    
    def test_conversion_score_recorded(self):
        """Тест: записавшийся клиент = 100% конверсия."""
        score = self.lm_service._calculate_conversion_score(
            self.test_call_score
        )
        self.assertEqual(score, 100.0)
    
    def test_conversion_score_lead_no_record(self):
        """Тест: лид без записи = 50% конверсия."""
        lead_call = self.test_call_score.copy()
        lead_call['outcome'] = 'lead_no_record'
        lead_call['call_category'] = 'Лид (без записи)'
        
        score = self.lm_service._calculate_conversion_score(lead_call)
        self.assertEqual(score, 50.0)
    
    def test_conversion_score_info_call(self):
        """Тест: информационный звонок = низкая конверсия."""
        info_call = self.test_call_score.copy()
        info_call['outcome'] = 'info_only'
        info_call['call_category'] = 'Информационный'
        
        score = self.lm_service._calculate_conversion_score(info_call)
        self.assertEqual(score, 20.0)
    
    def test_lost_opportunity_target_not_converted(self):
        """Тест: целевой звонок без конверсии = высокие потери."""
        lost_call = self.test_call_score.copy()
        lost_call['outcome'] = 'no_interest'
        lost_call['is_target'] = 1
        
        loss_score, loss_meta = self.lm_service._calculate_lost_opportunity(self.test_call_history, lost_call)
        self.assertEqual(loss_score, 80.0)
        self.assertIsInstance(loss_meta, dict)
    
    def test_lost_opportunity_full_penalty(self):
        """Тест: длинный разговор, низкий скор, нет причины = 100 баллов."""
        lost_call = self.test_call_score.copy()
        lost_call['outcome'] = 'lead_no_record'
        lost_call['is_target'] = 1
        lost_call['call_score'] = 3.5
        lost_call['refusal_reason'] = ''
        history = self.test_call_history.copy()
        history['talk_duration'] = 45
        
        loss_score, loss_meta = self.lm_service._calculate_lost_opportunity(history, lost_call)
        self.assertEqual(loss_score, 100.0)
        self.assertTrue(loss_meta.get("requires_reason"))
    
    def test_lost_opportunity_converted(self):
        """Тест: записавшийся клиент = нет потерь."""
        loss_score, loss_meta = self.lm_service._calculate_lost_opportunity(
            self.test_call_history,
            self.test_call_score
        )
        self.assertEqual(loss_score, 0.0)
        self.assertIsInstance(loss_meta, dict)
    
    def test_cross_sell_potential_booked(self):
        """Тест: записавшийся клиент = высокий cross-sell."""
        potential = self.lm_service._calculate_cross_sell_potential(
            self.test_call_score
        )
        self.assertEqual(potential, 70.0)
    
    # =================================================================
    # QUALITY METRICS TESTS
    # =================================================================
    
    def test_checklist_coverage(self):
        """Тест: покрытие чек-листа."""
        coverage = self.lm_service._calculate_checklist_coverage(
            self.test_call_score
        )
        # 8 / 10 * 100 = 80
        self.assertEqual(coverage, 80.0)
    
    def test_normalized_score_scale_10(self):
        """Тест: нормализация скора из шкалы 0-10."""
        normalized = self.lm_service._calculate_normalized_score(
            self.test_call_score
        )
        # 8.5 * 10 = 85
        self.assertEqual(normalized, 85.0)
    
    def test_script_risk_high_score(self):
        """Тест: высокий скор = низкий риск."""
        risk = self.lm_service._calculate_script_risk(
            self.test_call_score
        )
        # score=8.5 > 7, risk = 10.0
        self.assertEqual(risk, 10.0)
    
    def test_script_risk_low_score(self):
        """Тест: низкий скор = высокий риск."""
        low_score_call = self.test_call_score.copy()
        low_score_call['call_score'] = 2.0
        
        risk = self.lm_service._calculate_script_risk(low_score_call)
        # score=2.0 <= 3, risk = 80.0
        self.assertEqual(risk, 80.0)
    
    # =================================================================
    # RISK METRICS TESTS
    # =================================================================
    
    def test_churn_risk_complaint(self):
        """Тест: жалоба = высокий риск оттока."""
        complaint_call = self.test_call_score.copy()
        complaint_call['call_category'] = 'Жалоба'
        
        level, score = self.lm_service._calculate_churn_risk(complaint_call)
        self.assertEqual(level, 'high')
        self.assertEqual(score, 90.0)
    
    def test_churn_risk_cancellation(self):
        """Тест: отмена = средний риск оттока."""
        cancel_call = self.test_call_score.copy()
        cancel_call['call_category'] = 'Отмена записи'
        cancel_call['outcome'] = 'cancel'
        
        level, score = self.lm_service._calculate_churn_risk(cancel_call)
        self.assertEqual(level, 'high')  # 70 >= 70
        self.assertEqual(score, 70.0)
    
    def test_churn_risk_booked(self):
        """Тест: запись = низкий риск оттока."""
        level, score = self.lm_service._calculate_churn_risk(
            self.test_call_score
        )
        self.assertEqual(level, 'low')
        self.assertEqual(score, 10.0)
    
    def test_complaint_risk_direct(self):
        """Тест: прямая жалоба = 100% риск."""
        complaint_call = self.test_call_score.copy()
        complaint_call['call_category'] = 'Жалоба'
        complaint_call['transcript'] = 'Почему вы не записали меня? Я буду жаловаться руководству.'
        history = self.test_call_history.copy()
        history['talk_duration'] = 90
        
        score, flag, context = self.lm_service._calculate_complaint_risk(
            history,
            complaint_call,
        )
        self.assertGreaterEqual(score, 70.0)
        self.assertTrue(flag)
        self.assertIn("жалоба", " ".join(context.get("reasons", [])).lower())
    
    def test_complaint_risk_low_quality(self):
        """Тест: низкий скор без претензии не должен давать жалобу."""
        low_quality_call = self.test_call_score.copy()
        low_quality_call['call_score'] = 2.0
        low_quality_call['talk_duration'] = 80
        low_quality_call['transcript'] = 'Здравствуйте. Интересует запись. Спасибо.'
        
        score, flag, context = self.lm_service._calculate_complaint_risk(
            self.test_call_history,
            low_quality_call,
        )
        self.assertEqual(score, 0.0)
        self.assertFalse(flag)
        self.assertIn("Нет признаков", " ".join(context.get("reasons", [])))
    
    def test_followup_needed_lead(self):
        """Тест: лид без записи требует фоллоу-ап."""
        lead_call = self.test_call_score.copy()
        lead_call['outcome'] = 'lead_no_record'
        
        flag, context = self.lm_service._calculate_followup_needed(self.test_call_history, lead_call)
        self.assertTrue(flag)
        self.assertIsNotNone(context)
        self.assertIn("reason", context)
    
    def test_followup_not_needed_booked(self):
        """Тест: запись не требует фоллоу-ап."""
        flag, context = self.lm_service._calculate_followup_needed(
            self.test_call_history,
            self.test_call_score
        )
        self.assertFalse(flag)
        self.assertIsNone(context)
    
    def test_followup_reason_refusal_code(self):
        """Тест: код отказа из списка => follow-up c причиной."""
        call = self.test_call_score.copy()
        call['refusal_category_code'] = 'PATIENT_WILL_CLARIFY'
        call['refusal_reason'] = 'Пациент уточнит и подумает'
        
        flag, context = self.lm_service._calculate_followup_needed(self.test_call_history, call)
        self.assertTrue(flag)
        self.assertIsNotNone(context)
        self.assertIn('PATIENT_WILL_CLARIFY', " ".join(context.get("reasons", [])))
    
    def test_followup_non_target_drop(self):
        """Тест: технический обрыв с реальным клиентом."""
        call = self.test_call_score.copy()
        call['outcome'] = 'non_target'
        call['transcript'] = 'Здравствуйте, хочу записаться'
        history = self.test_call_history.copy()
        history['talk_duration'] = 35
        
        flag, context = self.lm_service._calculate_followup_needed(history, call)
        self.assertTrue(flag)
        self.assertIn('non_target', " ".join(context.get("reasons", [])))
    
    # =================================================================
    # FORECAST METRICS TESTS
    # =================================================================
    
    def test_forecast_conversion_already_converted(self):
        """Тест: уже записался = 100% вероятность."""
        prob = self.lm_service._forecast_conversion_probability(
            self.test_call_score
        )
        self.assertEqual(prob, 1.0)
    
    def test_forecast_conversion_lead(self):
        """Тест: лид = 35% вероятность конверсии."""
        lead_call = self.test_call_score.copy()
        lead_call['outcome'] = 'lead_no_record'
        
        prob = self.lm_service._forecast_conversion_probability(lead_call)
        self.assertEqual(prob, 0.35)
    
    def test_forecast_second_call_navigation(self):
        """Тест: навигационный звонок = 60% вероятность повторного."""
        nav_call = self.test_call_score.copy()
        nav_call['call_category'] = 'Навигация'
        
        prob = self.lm_service._forecast_second_call_probability(nav_call)
        self.assertEqual(prob, 0.60)
    
    def test_forecast_complaint_already_complaint(self):
        """Тест: уже жалоба = 100% вероятность."""
        complaint_call = self.test_call_score.copy()
        complaint_call['call_category'] = 'Жалоба'
        
        prob = self.lm_service._forecast_complaint_probability(complaint_call)
        self.assertEqual(prob, 1.0)
    
    def test_forecast_complaint_low_score(self):
        """Тест: низкий скор = 40% вероятность жалобы."""
        low_score_call = self.test_call_score.copy()
        low_score_call['call_score'] = 2.0
        
        prob = self.lm_service._forecast_complaint_probability(low_score_call)
        self.assertEqual(prob, 0.40)


if __name__ == '__main__':
    # Запуск тестов
    unittest.main(verbosity=2)
