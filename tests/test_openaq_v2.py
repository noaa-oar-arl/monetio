import monetio.obs.openaq_v2 as openaq


def test_get_parameters():
    params = openaq.get_parameters()
    assert 50 <= len(params) <= 500
    assert params.id.nunique() == len(params)
    assert params.name.nunique() < len(params), "dupes for different units etc."
    assert "pm25" in params.name.values
    assert "o3" in params.name.values
