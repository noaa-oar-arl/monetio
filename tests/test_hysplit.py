import numpy as np

from monetio import hysplit


def test_003():
    """
    tests for getlatlon and  get_latlongrid functions.
    tests global grids.
    """
    # Longitude
    # center = 1
    # spacing = 0.5
    # span = 360
    # --------------------
    # Latitude
    # center = 1
    # spacing = 0.5
    # span = 180.0
    # ------------
    attrs = {
        "llcrnr latitude": -90.0,
        "llcrnr longitude": -180.0,
        "Latitude Spacing": 0.5,
        "Longitude Spacing": 0.5,
        "Number Lat Points": 361,
        "Number Lon Points": 721,
    }
    xxx = [1, 5, 93]
    xanswers = [165.0, 167.0, -149.0]
    yyy = [1, 17, 93]
    yanswers = [0.0, 8.0, 46.0]
    grid = hysplit.get_latlongrid(attrs, xxx, yyy)  # noqa: F841
    latlist, lonlist = hysplit.getlatlon(attrs)
    gridanswer = np.meshgrid(xanswers, yanswers)  # noqa: F841

    assert len(latlist) == 361
    assert len(lonlist) == 721
    # check that lat lon begin and end in correct place.
    assert latlist[0] == -90.0
    assert latlist[-1] == 90.0
    assert lonlist[0] == -180.0
    # should go -180 to 180 or -180 to -180?
    assert lonlist[-1] == 180.0
    assert lonlist[-2] == 179.5


def test_002():
    """
    tests for getlatlon and  get_latlongrid functions.
    tests medium grid with 90 degree span which crosses date line.
    """
    # Longitude
    # center  = -150
    # spacing =  0.5
    # span    = 90.0
    # ------------
    # crnr    = 165
    # nlon    = 181
    # x=1  longitude = 165
    # x=5  longitude = 167.0
    # x=93 longitude = -149.0

    # Latitude
    # center = 45
    # spacing = 0.5
    # span = 90.0
    # ------------
    # crnr = 0.0
    # nlat = 181
    # y=1  latitude=0.0
    # y=17 latitude=8
    # y=93 latitude=46.0

    attrs = {
        "llcrnr latitude": 0.0,
        "llcrnr longitude": 165,
        "Latitude Spacing": 0.5,
        "Longitude Spacing": 0.5,
        "Number Lat Points": 181.0,
        "Number Lon Points": 181.0,
    }
    xxx = [1, 5, 93]
    xanswers = [165.0, 167.0, -149.0]
    yyy = [1, 17, 93]
    yanswers = [0.0, 8.0, 46.0]
    grid = hysplit.get_latlongrid(attrs, xxx, yyy)
    latlist, lonlist = hysplit.getlatlon(attrs)

    gridanswer = np.meshgrid(xanswers, yanswers)
    assert np.array_equal(grid, gridanswer)

    assert len(latlist) == 181
    assert len(lonlist) == 181
    # check that lat lon begin and end in correct place.
    assert latlist[0] == 0.0
    assert latlist[-1] == 90.0
    assert lonlist[0] == 165
    assert lonlist[-1] == -105.0

    for x in zip(xxx, xanswers):
        assert lonlist[x[0] - 1] == x[1]

    for y in zip(yyy, yanswers):
        assert latlist[y[0] - 1] == y[1]


def test_001():
    """
    tests for getlatlon and  get_latlongrid functions.
    tests simple small grid with 10 degree span.
    """
    # Longitude
    # center  = -150
    # spacing =  0.1
    # span    = 10.0
    # ------------
    # crnr    = -155
    # nlon    = 101
    # x=1  longitude = -155
    # x=33 longitude = -151.8
    # x=69 longitude = -148.2

    # Latitude
    # center = 45
    # spacing = 0.1
    # span = 10.0
    # ------------
    # crnr = 40.0
    # nlat = 101
    # y=1  latitude=40.0
    # y=50 latitude=44.9
    # y=73 latitude=47.2

    attrs = {
        "llcrnr latitude": 40.0,
        "llcrnr longitude": -155,
        "Latitude Spacing": 0.1,
        "Longitude Spacing": 0.1,
        "Number Lat Points": 101.0,
        "Number Lon Points": 101.0,
    }

    xxx = [1, 33, 69]
    yyy = [1, 50, 73]
    xanswers = [-155, -151.8, -148.2]
    yanswers = [40.0, 44.9, 47.2]
    grid = hysplit.get_latlongrid(attrs, xxx, yyy)
    gridanswer = np.meshgrid(xanswers, yanswers)
    assert np.array_equal(grid, gridanswer)

    latlist, lonlist = hysplit.getlatlon(attrs)
    assert len(latlist) == 101
    assert len(lonlist) == 101
    assert latlist[0] == 40.0
    assert latlist[-1] == 50.0
    assert lonlist[0] == -155.0
    assert lonlist[-1] == -145.0

    for x in zip(xxx, xanswers):
        assert lonlist[x[0] - 1] == x[1]

    for y in zip(yyy, yanswers):
        assert latlist[y[0] - 1] == y[1]


# test_001()
