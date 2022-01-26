# server = 'ftp.star.nesdis.noaa.gov'
# base_dir = https://gsce-dtn.sdstate.edu/index.php/s/e8wPYPOL1bGXk5z/download?path=%2F20190318&files=meanFRP.20190318.FV3.C384Grid.tile6.bin
base_dir = "https://gsce-dtn.sdstate.edu/index.php/s/e8wPYPOL1bGXk5z/download?path=%2F"


def download_data(date, ftype="meanFRP"):
    from datetime import datetime

    import requests as rq
    from numpy import arange

    if isinstance(date, datetime):
        yyyymmdd = date.strftime("%Y%m%d")
    else:
        from pandas import Timestamp

        date = Timestamp(date)
        yyyymmdd = date.strftime("%Y%m%d")

    url_ftype = f"&files={ftype}."

    for i in arange(1, 7, dtype=int).astype(str):
        tile = f".FV3C384Grid.tile{i}.bin"
        url = f"{base_dir}{yyyymmdd}{url_ftype}{yyyymmdd}{tile}"
        fname = f"{ftype}.{yyyymmdd}.FV3.C384Grid.tile{i}.bin"
        print("Retrieving file:", fname)
        r = rq.get(url)
        with open(fname, "wb") as f:
            f.write(r.content)
