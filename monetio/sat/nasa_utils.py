import os

import requests


class SessionWithHeaderRedirection(requests.Session):
    """NASA Session generator

    Parameters
    ----------
    username : type
        Description of parameter `username`.
    password : type
        Description of parameter `password`.

    Attributes
    ----------
    auth : type
        Description of attribute `auth`.
    AUTH_HOST : type
        Description of attribute `AUTH_HOST`.

    """

    AUTH_HOST = "urs.earthdata.nasa.gov"

    def __init__(self, username, password):
        super().__init__()
        self.auth = (username, password)

    # Overrides from the library to keep headers when redirected to or from
    # the NASA auth host.
    def rebuild_auth(self, prepared_request, response):
        headers = prepared_request.headers
        url = prepared_request.url
        if "Authorization" in headers:
            original_parsed = requests.utils.urlparse(response.request.url)
            redirect_parsed = requests.utils.urlparse(url)
            if (
                (original_parsed.hostname != redirect_parsed.hostname)
                and redirect_parsed.hostname != self.AUTH_HOST
                and original_parsed.hostname != self.AUTH_HOST
            ):
                del headers["Authorization"]

        return


def get_nasa_data(username, password, filename):
    session = SessionWithHeaderRedirection(username, password)

    # the url of the file we wish to retrieve
    url = filename
    filename = filename.split(os.sep)[-1]
    try:
        # submit the request using the session
        response = session.get(url, stream=True)
        print(response.status_code)
        # raise an exception in case of http errors
        response.raise_for_status()
        # save the file
        with open(filename, "wb") as fd:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                fd.write(chunk)
    except requests.exceptions.HTTPError as e:
        # handle any errors here
        print(e)
        return None


def get_filenames_http(archive_url, ext):
    from bs4 import BeautifulSoup

    r = requests.get(archive_url)
    soup = BeautifulSoup(r.content, "html.parser")
    links = soup.findAll("a")
    return [archive_url + link["href"] for link in links if link["href"].endswith("%s" % ext)]


def get_available_satellites(archive_url="https://e4ftl01.cr.usgs.gov"):
    return get_filenames_http(archive_url, "/")


def get_available_product(archive_url="https://e4ftl01.cr.usgs.gov", satellite=None):
    url = f"{archive_url}/{satellite}"
    return get_filenames_http(url, "/")


def get_files_to_download(year, doy, tiles, output_path, ext, sat="MOLA", product="MYD09A1.006"):
    import pandas as pd
    from numpy import array

    # startdd = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
    # enddd = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()
    # num_days = (enddd - startdd).days
    # list files available from website
    # d = datetime.date((year - 1), 12, 31) + datetime.timedelta(days=doy)
    d = pd.Timestamp(year, doy)
    # doy = (dd - datetime.date(dd.year - 1, 12, 31)).days
    # year = dd.year
    baseurl = "https://e4ftl01.cr.usgs.gov"
    archive_url = "{}/{}/{}/{}/".format(baseurl, sat, product, d.strftime("%Y.%m.%d"))
    # archive_url = 'https://e4ftl01.cr.usgs.gov/MOLA/MYD09A1.006/%d.%02d.%02d/'
    #     sat,product, d.strftime('%Y.%m.%d'))
    files = get_filenames_http(archive_url, ext)
    #    print(files)
    #    files_on_http = []
    #    for f in files:
    #        files_on_http.append(f)
    # for tile in tiles:
    #    files_on_http.append(
    #        [f for f in files if '%d%03d.%s' % (year, doy, tile) in f])
    #    files_on_http2 = set([str(x[0]) for x in files_on_http if x])
    #    files_on_system = set(glob.glob(os.path.join(output_path, "*.%s" % ext)))
    #    return list(files_on_http2 - files_on_system)
    # GET THE FILES NOT CURRENTLY ON THE SYSTEM
    basenames = [os.path.basename(f) for f in files]
    files_on_system = [os.path.isfile(f"{output_path}/{f}") for f in basenames]
    files_to_download = array(files)[~array(files_on_system)]
    return files_to_download, basenames
