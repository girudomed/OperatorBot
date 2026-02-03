import re
from typing import Optional

def _sanitize_sql(query: Optional[str]) -> Optional[str]:
    """
    Горячий фикс: в старых запросах могли остаться обращения к cs.score.
    В MySQL такого столбца нет (есть call_score), поэтому мягко переписываем SQL.
    """
    if not isinstance(query, str) or "score" not in query:
        return query
    pattern = re.compile(r"(?i)\b(cs|call_scores)\.score\b")
    return pattern.sub(lambda match: f"{match.group(1)}.call_score", query)

def test_sanitize():
    print(f"Testing _sanitize_sql...")
    
    # Test 1: Normal query
    q1 = "SELECT * FROM call_scores cs WHERE cs.score > 5"
    res1 = _sanitize_sql(q1)
    print(f"Q1: '{q1}' -> '{res1}'")
    assert "cs.call_score" in res1
    
    # Test 2: No match
    q2 = "SELECT * FROM users"
    res2 = _sanitize_sql(q2)
    print(f"Q2: '{q2}' -> '{res2}'")
    assert res2 == q2
    
    # Test 3: None
    q3 = None
    res3 = _sanitize_sql(q3)
    print(f"Q3: {q3} -> {res3}")
    assert res3 is None
    
    # Test 4: Empty string
    q4 = ""
    res4 = _sanitize_sql(q4)
    print(f"Q4: '{q4}' -> '{res4}'")
    assert res4 == ""
    
    # Test 5: Call_scores.score
    q5 = "SELECT call_scores.score FROM call_scores"
    res5 = _sanitize_sql(q5)
    print(f"Q5: '{q5}' -> '{res5}'")
    assert "call_scores.call_score" in res5

    print("All tests passed")

if __name__ == "__main__":
    test_sanitize()
