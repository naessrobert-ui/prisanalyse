from bil_routes import _solgt_true_expr


def test_solgt_true_expr_utelukker_fjernet_status():
    expr = _solgt_true_expr("status_norm")

    assert "'solgt'" in expr
    assert "'sold'" in expr
    assert "'fjernet'" not in expr
    assert "'removed'" not in expr
