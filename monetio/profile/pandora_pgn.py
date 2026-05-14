import datetime as dt
from glob import glob

import pandas as pd
import xarray as xr


def _parse_metadata(value):
    """Parse metadata to possible values.

    Parameters
    ----------
    value : str
        str to parse

    Returns
    -------
    int | float | datetime | str
        parsed data
    """
    if value == "":  # Deal with the empty string as a special case
        return value
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        pass
    try:
        return pd.to_datetime(value, format="ISO8601").to_datetime64()
    except ValueError:
        return value.lstrip().rstrip()


def _name_columns(index_number):
    """Creates the names for columns.

    Parameters
    ----------
    index_number : int
        Number originally from the df index

    Returns
    -------
    str
        str as Column {index_number + 1}
    """
    return f"Column {index_number + 1}"


def _rename_and_format(df):
    """Renames each variable with the column number, following the convention of PGN files.
    It formats the data as an xarray.Dataset and adds the x dimension

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe with columns to rename

    Returns
    -------
    xr.Dataset
        Dataset with the renamed data
    """
    df2 = df.rename(_name_columns, axis="columns")
    df2 = df2.rename(columns={"Column 1": "time"})
    df2 = df2.set_index("time")
    return df2.to_xarray().expand_dims("x", axis=1)


def _parse_file_to_df(file_path):
    """Parse a Pandora PGN file to a DataFrame.

    Only the header lines are read in Python; the data section is passed
    directly to the pandas C parser via :func:`pandas.read_csv`.

    Global metadata and column-header descriptions are stored in
    ``df.attrs`` under ``"_global_attrs"`` and ``"_headers"`` respectively.

    Parameters
    ----------
    file_path : str or Path
        Path to a single Pandora PGN text file.

    Returns
    -------
    pd.DataFrame
        Column 0 contains parsed datetimes; remaining columns are float.
    """
    count_line_dividers = 0
    global_attrs = {
        "history": f"{dt.datetime.now()}: created from _read_pandora_files, pandora_pgn.py"
    }
    headers = {}
    data_start_line = None

    with open(file_path, encoding="latin-1") as f:
        for line_num, line in enumerate(f):
            line_stripped = line.rstrip()
            if line_stripped.startswith("-----------"):
                count_line_dividers += 1
                if count_line_dividers == 2:
                    data_start_line = line_num + 1
                    break
            elif count_line_dividers == 0:
                attr_name, value = line_stripped.split(":", 1)
                global_attrs[attr_name] = _parse_metadata(value)
            elif count_line_dividers == 1:
                key, metadata = line_stripped.split(":", 1)
                headers[key] = metadata
        else:
            raise ValueError("File ended before data section was reached")

    # Number of standard (non-optional) columns is the count of "Column N" header keys.
    # Some files have additional variable-length trailing fields per row described by
    # "From Column N" in the headers; we ignore those.
    n_cols = sum(1 for k in headers if k.startswith("Column "))

    df = pd.read_csv(
        file_path,
        engine="c",
        sep=" ",
        header=None,
        usecols=range(n_cols),
        skiprows=data_start_line,
        encoding="latin-1",
    )

    df[0] = pd.to_datetime(df[0], format="ISO8601").dt.tz_localize(None)
    df.iloc[:, 1:] = df.iloc[:, 1:].apply(pd.to_numeric, errors="coerce")
    df.attrs["_global_attrs"] = global_attrs
    df.attrs["_headers"] = headers

    return df


def _df_to_ds(df):
    """Convert a parsed Pandora DataFrame to an :class:`xr.Dataset`.

    Parameters
    ----------
    df : pd.DataFrame
        As returned by :func:`_parse_file_to_df`.

    Returns
    -------
    xr.Dataset
        Dataset formatted for MELODIES-MONET.
    """
    global_attrs = df.attrs["_global_attrs"]
    headers = df.attrs["_headers"]

    data = _rename_and_format(df)
    data["latitude"] = (("x",), [global_attrs["Location latitude [deg]"]])
    data["latitude"].attrs["units"] = "degrees_north"
    data["longitude"] = (("x",), [global_attrs["Location longitude [deg]"]])
    data["longitude"].attrs["units"] = "degrees_east"
    data.attrs = global_attrs
    for k in headers:
        if k in data:
            data[k].attrs["description"] = headers[k]
        elif k.startswith("From Column"):
            optional_keys = headers[k]
    non_shared_keys = list(set(data.keys()) - set(headers.keys()))
    non_shared_keys.remove("latitude")
    non_shared_keys.remove("longitude")
    if len(non_shared_keys) > 0:
        for k in non_shared_keys:
            data[k].attrs["description"] = optional_keys
    data["siteid"] = (("x",), [data.attrs["Short location name"]])
    data = data.assign_coords({"longitude": data["longitude"], "latitude": data["latitude"]})
    return data


def _read_pandora_file(file_path):
    """Read a Pandora PGN file as an :class:`xr.Dataset`.

    Parameters
    ----------
    file_path : str or Path
        Path to a single Pandora PGN text file.

    Returns
    -------
    xr.Dataset
        Dataset from single file formatted for MELODIES-MONET.
    """
    return _df_to_ds(_parse_file_to_df(file_path))


def _merge_global_attrs(ds1, ds2, merged):
    """Merges global attributes of two datasets as a list and adds
    them to the merged dataset inplace

    Parameters
    ----------
    ds1 : xr.Dataset
        First dataset
    ds2 : xr.Dataset
        Second dataset
    merged : xr.Dataset
        merged Dataset, the global attributes will be assigned as
        lists of ds1.attrs + ds2.attrs

    Returns
    -------
    None
    """
    for k in merged.attrs:
        merged.attrs[k] = list([ds1.attrs.get(k, "")]) + list([ds2.attrs.get(k, "")])


def open_mfdataset(path):
    """Opens multiple Pandora PGN files and combines them to
    MELODIES-MONET compatible format.

    Parameters
    ----------
    path: str
        String containing the paths

    Returns
    -------
    xr.Dataset
        Formatted dataset. Should work for MELODIES-MONET.
    """
    if isinstance(path, str):
        files = sorted(glob(path))
    if isinstance(path, list):
        files = []
        for file in path:
            files = files + list(glob(str(file)))
        files = sorted(files)
    ds = _read_pandora_file(files[0])
    if len(files) > 1:
        for f in files[1:]:
            ds2 = _read_pandora_file(f)
            if ds.attrs["Data file version"] != ds2.attrs["Data file version"]:
                raise Exception("Different data file versions, cannot concatenate")
            if ds.attrs["Short location name"] != ds2.attrs["Short location name"]:
                ds = xr.concat([ds, ds2], dim="x")
            else:
                ds = xr.concat([ds, ds2], dim="time")
            _merge_global_attrs(ds, ds2, ds)
        ds.attrs["history"] = [
            f"{dt.datetime.now()}: open_mfdataset from pandora_pgn.py "
        ] + ds.attrs["history"]
    return ds
