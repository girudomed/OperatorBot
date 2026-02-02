from pathlib import Path

from app.services.lm_weights import ComplaintWeightMatrix, DEFAULT_MATRIX


def test_lm_weights_load_invalid_json_falls_back(tmp_path):
    bad_file = tmp_path / "lm_weight_matrix.json"
    bad_file.write_text("{not-json", encoding="utf-8")

    matrix = ComplaintWeightMatrix(path=str(bad_file))

    assert matrix.thresholds == DEFAULT_MATRIX["thresholds"]
