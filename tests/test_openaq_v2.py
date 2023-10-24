import monetio.obs.openaq_v2 as openaq


def test_get_parameters():
    params = openaq.get_parameters()
    assert 50 <= len(params) <= 500
    assert params.id.nunique() == len(params)
    assert params.name.nunique() < len(params), "dupes for different units etc."
    assert "pm25" in params.name.values
    assert "o3" in params.name.values


def test_get_locations():
    sites = openaq.get_locations(npages=2, limit=100)
    assert len(sites) <= 200
    assert sites.siteid.nunique() == len(sites)
    assert sites.dtypes["firstUpdated"] == "datetime64[ns]"
    assert sites.dtypes["lastUpdated"] == "datetime64[ns]"
    assert sites.dtypes["latitude"] == "float64"
    assert sites.dtypes["longitude"] == "float64"
    assert sites["latitude"].isnull().sum() == 0
    assert sites["longitude"].isnull().sum() == 0
