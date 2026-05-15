import datetime as dt
import re
from glob import glob

import numpy as np
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
        ``"From Column N"`` description). Uses a high-memory line-by-line read
        so that ragged rows (varying layer counts) are NaN-padded correctly.
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
        # Read lines manually so ragged rows (varying layer counts) are handled:
        # pd.DataFrame from a list of lists NaN-pads shorter rows automatically.
        rows = []
        with open(file_path, encoding="latin-1") as f:
            for i, line in enumerate(f):
                if i >= data_start_line:
                    rows.append(line.split())
        raw = pd.DataFrame(rows)
        time = pd.to_datetime(raw[0], format="ISO8601").dt.tz_localize(None)
        numeric = raw.iloc[:, 1:].apply(pd.to_numeric, errors="coerce")
        df = pd.concat([time, numeric], axis=1)
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
        # Note C engine auto-detects numeric types

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


def _add_layer_data(ds, df_extra, col_descs):
    """Promote layer columns to ``(time, x, z)`` variables spanning all layers.

    Layer 1 data is already loaded in ``ds`` as ``(time, x)`` variables.
    This function replaces those variables with ``(time, x, z)`` ones where
    ``z=0`` is layer 1 and ``z=1, 2, ...`` are the optional higher layers from
    ``df_extra``.  Variable names and descriptions follow the file header
    (e.g. ``col53``, ``col54`` for the HCHO 2-column-per-layer product).

    Parameters
    ----------
    ds : xr.Dataset
        Dataset built from the standard columns by :func:`_df_to_ds`.
    df_extra : pd.DataFrame
        Trailing optional columns beyond the standard ``n_cols``
        (positional indexing, i.e. the result of ``df_full.iloc[:, n_cols:]``).
    col_descs : dict
        Column description mapping from ``df.attrs["_col_descs"]``.

    Returns
    -------
    xr.Dataset
        ``ds`` with the layer-1 ``(time, x)`` variables replaced by
        ``(time, x, z)`` variables covering all retrieved layers.
    """
    from_col_key = next((k for k in col_descs if k.startswith("From Column")), None)
    if from_col_key is None or df_extra.shape[1] == 0:
        return ds

    from_col_num = int(from_col_key.split()[2])  # "From Column 55" → 55
    m = re.search(r"(\d+) columns? per layer", col_descs[from_col_key])
    if m is None:
        return ds
    stride = int(m.group(1))

    n_cols = sum(1 for k in col_descs if k.startswith("Column "))
    width = len(str(n_cols))

    # Layer 1 occupies the `stride` standard columns immediately before "From Column N"
    layer1_col_nums = range(from_col_num - stride, from_col_num)  # e.g. [53, 54]
    max_extra_layers = df_extra.shape[1] // stride  # number of optional (layer 2+) layers

    for pos, col_num in enumerate(layer1_col_nums):
        col_name = f"col{col_num:0{width}d}"

        # Layer 1: already in ds as (time, x)
        layer1 = ds[col_name].values  # (time, x=1)

        # Layers 2+: every `stride`-th column starting at position `pos`
        extra_indices = list(range(pos, max_extra_layers * stride, stride))
        extra = df_extra.iloc[:, extra_indices].to_numpy()  # (time, max_extra_layers)

        # Concatenate along the new z axis: (time, x=1, total_layers)
        all_layers = np.concatenate([layer1[:, :, np.newaxis], extra[:, np.newaxis, :]], axis=2)

        desc = re.sub(r" layer 1\b", " layer", col_descs.get(f"Column {col_num}", ""))
        ds[col_name] = (("time", "x", "z"), all_layers, {"description": desc})

    return ds


def open_dataset(file_path, layers=False):
    """Read a Pandora PGN file as an :class:`xr.Dataset`.

    Parameters
    ----------
    file_path : str or Path
        Path to a single Pandora PGN text file.
    layers : bool, optional
        If True, also parse the optional higher-layer results (profile data)
        and include them as ``layer_col1``, ``layer_col2``, ... variables
        with a ``z`` (layer) dimension. Requires the slower Python CSV engine.
        Default False.

    Returns
    -------
    xr.Dataset
        Dataset from single file formatted for MELODIES MONET.
        If ``layers=True``, includes a ``z`` dimension for profile levels.
    """
    if layers:
        df_full = _parse_file_to_df(file_path, include_optional_cols=True)
        col_descs = df_full.attrs["_col_descs"]
        n_cols = sum(1 for k in col_descs if k.startswith("Column "))
        df_std = df_full.iloc[:, :n_cols].copy()
        df_std.attrs = df_full.attrs.copy()
        ds = _df_to_ds(df_std)
        ds = _add_layer_data(ds, df_full.iloc[:, n_cols:], col_descs)
    else:
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
