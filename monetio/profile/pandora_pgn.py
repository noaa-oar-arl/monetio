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
        return pd.to_datetime(value, format="ISO8601")
    except ValueError:
        return value.lstrip().rstrip()


def _rename_and_format(df):
    """Rename columns to zero-padded names and set time as the index.

    Column 0 becomes the time index; remaining columns are named ``col01``,
    ``col02``, etc., with zero-padding width determined by the total column count.

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe with integer column labels (0, 1, 2, ...).

    Returns
    -------
    pd.DataFrame
        Renamed dataframe with time as the index.
    """
    n = len(df.columns)
    width = len(str(n))
    rename_map = {0: "time", **{i: f"col{i+1:0{width}d}" for i in range(1, n)}}
    df2 = df.rename(columns=rename_map).set_index("time")
    df2.attrs = df.attrs
    return df2


def _parse_file_to_df(file_path, include_optional_cols=False):
    """Parse a Pandora PGN file to a DataFrame.

    Only the header lines are read in Python; the data section is passed
    to :func:`pandas.read_csv`.

    Global metadata and column-header descriptions are stored in
    ``df.attrs`` under ``"_global_attrs"`` and ``"_col_descs"`` respectively.

    Parameters
    ----------
    file_path : str or Path
        Path to a single Pandora PGN text file.
    include_optional_cols : bool, optional
        If True, include the optional higher-layer results described by
        ``"From Column N"`` in the file header. These are variable-length
        trailing fields giving per-layer top height and partial column amount
        for each retrieved profile level (stride defined in the
        ``"From Column N"`` description). Requires the slower Python CSV
        engine; rows with fewer layers are NaN-padded to the longest row.
        Default False.

    Returns
    -------
    pd.DataFrame
        Column 0 contains parsed datetimes; remaining columns are float.
    """
    count_line_dividers = 0
    global_attrs = {
        "history": f"{dt.datetime.now(dt.timezone.utc).isoformat()}: created from monetio pandora_pgn.py"
    }
    col_descs = {}
    data_start_line = None

    with open(file_path, encoding="latin-1") as f:
        for line_num, line in enumerate(f):
            line_stripped = line.rstrip()
            if line_stripped.startswith("-----------"):
                count_line_dividers += 1
                if count_line_dividers == 2:
                    data_start_line = line_num + 1
                    break
            elif count_line_dividers == 0:  # File metadata
                attr_name, value = line_stripped.split(":", 1)
                global_attrs[attr_name] = _parse_metadata(value)
            elif count_line_dividers == 1:  # Column descriptions
                key, desc = line_stripped.split(":", 1)
                col_descs[key] = desc
        else:
            raise ValueError("File ended before data section was reached")

    # Number of standard columns is the count of "Column N" header keys.
    # Some files also have optional higher-layer results described by
    # "From Column N", e.g. giving per-layer top height and partial column amount.
    n_cols = sum(1 for k in col_descs if k.startswith("Column "))

    if include_optional_cols:
        # Python engine handles ragged rows: shorter rows are NaN-padded to the longest.
        df = pd.read_csv(
            file_path,
            engine="python",
            sep=r"\s+",
            header=None,
            skiprows=data_start_line,
            encoding="latin-1",
        )
    else:
        # Faster C engine requires a consistent column count, so only read the standard columns.
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
    df.attrs["_col_descs"] = col_descs

    return df


def read_txt(file_path, include_optional_cols=False):
    """Parse a Pandora PGN text file to a :class:`pandas.DataFrame`.

    Parameters
    ----------
    file_path : str or Path
        Path to a single Pandora PGN text file.
    include_optional_cols : bool, optional
        If True, include the optional higher-layer results described by
        ``"From Column N"`` in the file header. These give per-layer top
        height and partial column amount for each retrieved profile level,
        with the number of columns per layer defined in the
        ``"From Column N"`` description. Requires the slower Python CSV
        engine. Default False.

    Returns
    -------
    pd.DataFrame
        Columns named ``col01``, ``col02``, etc. with ``time`` as the index.
        Global metadata and column-header descriptions are in ``df.attrs``
        under ``"_global_attrs"`` and ``"_col_descs"``.
    """
    return _rename_and_format(
        _parse_file_to_df(file_path, include_optional_cols=include_optional_cols)
    )


def _df_to_ds(df):
    """Convert a parsed Pandora DataFrame to an :class:`xr.Dataset`.

    Parameters
    ----------
    df : pd.DataFrame
        As returned by :func:`_parse_file_to_df`.

    Returns
    -------
    xr.Dataset
        Dataset formatted for MELODIES MONET.
    """
    global_attrs = df.attrs["_global_attrs"]
    col_descs = df.attrs["_col_descs"]
    width = len(str(len(df.columns)))

    ds = _rename_and_format(df).to_xarray().expand_dims("x", axis=1)
    ds["latitude"] = (
        ("x",),
        [global_attrs["Location latitude [deg]"]],
        {"long_name": "latitude", "units": "degrees_north"},
    )
    ds["longitude"] = (
        ("x",),
        [global_attrs["Location longitude [deg]"]],
        {"long_name": "longitude", "units": "degrees_east"},
    )
    ds["altitude"] = (
        ("x",),
        [float(global_attrs["Location altitude [m]"])],
        {"long_name": "altitude", "units": "m"},
    )
    ds.attrs = global_attrs
    standard_col_names = set()
    optional_keys = None
    for k, desc in col_descs.items():
        if k.startswith("From Column"):
            optional_keys = desc
        elif k.startswith("Column "):
            col_num = int(k.split()[1])
            col_name = f"col{col_num:0{width}d}"
            standard_col_names.add(col_name)
            if col_name in ds:
                ds[col_name].attrs["description"] = desc
    non_shared_keys = list(
        set(ds.keys()) - standard_col_names - {"latitude", "longitude", "altitude"}
    )
    if non_shared_keys and optional_keys is not None:
        for k in non_shared_keys:
            ds[k].attrs["description"] = optional_keys
    ds["siteid"] = (
        ("x",),
        [ds.attrs["Short location name"]],
        {"long_name": "site ID"},
    )
    ds = ds.set_coords(["latitude", "longitude", "altitude"])
    for k, v in ds.attrs.items():
        # Convert to string so we can save as nc
        if isinstance(v, pd.Timestamp):
            ds.attrs[k] = v.isoformat()
    return ds


def open_dataset(file_path):
    """Read a Pandora PGN file as an :class:`xr.Dataset`.

    Parameters
    ----------
    file_path : str or Path
        Path to a single Pandora PGN text file.

    Returns
    -------
    xr.Dataset
        Dataset from single file formatted for MELODIES MONET.
    """
    ds = _df_to_ds(_parse_file_to_df(file_path))
    ds.attrs["history"] = (
        f"{dt.datetime.now(dt.timezone.utc).isoformat()}: open_dataset from monetio pandora_pgn.py"
    )
    return ds


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


def open_mfdataset(file_path):
    """Opens multiple Pandora PGN files and combines them to
    MELODIES MONET compatible format.

    Parameters
    ----------
    file_path : str or list
        Path(s) to Pandora PGN text file(s).

    Returns
    -------
    xr.Dataset
        Formatted dataset. Should work for MELODIES MONET.
    """
    if isinstance(file_path, str):
        files = sorted(glob(file_path))
    if isinstance(file_path, list):
        files = []
        for file in file_path:
            files = files + list(glob(str(file)))
        files = sorted(files)
    ds = open_dataset(files[0])
    if len(files) > 1:
        for f in files[1:]:
            ds2 = open_dataset(f)
            if ds.attrs["Data file version"] != ds2.attrs["Data file version"]:
                raise Exception("Different data file versions, cannot concatenate")
            if ds.attrs["Short location name"] != ds2.attrs["Short location name"]:
                ds = xr.concat([ds, ds2], dim="x")
            else:
                ds = xr.concat([ds, ds2], dim="time")
            _merge_global_attrs(ds, ds2, ds)
        ds.attrs["history"] = (
            f"{dt.datetime.now(dt.timezone.utc).isoformat()}: open_mfdataset from monetio pandora_pgn.py"
            f"\n{ds.attrs['history']}"
        )
    return ds
