from app.telegram.ui.admin.screens.lm_screens import _describe_action_item


def test_describe_action_item_handles_bad_json():
    reasons, action = _describe_action_item(
        "followup",
        {"value_json": "{bad-json}", "result": "ok"},
    )

    assert isinstance(reasons, str)
    assert isinstance(action, str)
