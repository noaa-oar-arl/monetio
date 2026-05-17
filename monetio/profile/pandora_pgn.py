import datetime as dt
import re
import warnings
from glob import glob

import numpy as np
import pandas as pd
import xarray as xr

_BASE_URL = "https://api.pandonia-global-network.org/v1"


_HEADERS = {"User-Agent": "monetio"}


def get_locations():
    """Return all available PGN locations from the web API.

    Returns
    -------
    pd.DataFrame
        One row per location with columns
        ``name``, ``long_name``, ``lat``, ``lon``, ``alt``, and ``aliases``.
    """
    import requests

    r = requests.get(f"{_BASE_URL}/files/locations", headers=_HEADERS)
    r.raise_for_status()

    return pd.DataFrame(r.json())


def get_location_files(location, dates, *, level="L2", prod=None):
    """Return available PGN file metadata for a location and date range.

    Traverses the four-level API hierarchy:
    location → instruments (pan_id) → spectrometers → files.

    Parameters
    ----------
    location : str
        Location short name as returned by :func:`get_locations`,
        e.g. ``"Innsbruck"``.
    dates : datetime-like or array-like of datetime-like
        One date or an array; min and max are used as the inclusive time bounds
        (``start`` / ``end``) passed to the API.
    level : str, optional
        Data level. One of ``"L0"``, ``"L1"``, ``"L2Fit"``, ``"L2"``,
        ``"L2Geoms"``. Default ``"L2"``.
    prod : str, optional
        Product code filter (e.g. ``"rnvs3"``).
        Default: no filter (all products returned).

    Returns
    -------
    pd.DataFrame
        One row per file. Columns include ``filename``, ``size``,
        ``created_time``, ``modified_time``, ``metadata_date``,
        ``metadata_code``, ``pan_id``, ``spectrometer``, ``location``, and ``level``.
        Empty DataFrame if no files are found.
    """
    import requests

    dates = pd.to_datetime(dates)
    if pd.api.types.is_scalar(dates):
        dates = pd.DatetimeIndex([dates])
    start = dates.min().isoformat()
    end = dates.max().isoformat()

    # Step 1: instruments at the location
    r = requests.get(f"{_BASE_URL}/files/{location}", headers=_HEADERS)
    r.raise_for_status()
    instruments = [d["pan_id"] for d in r.json()]

    rows = []
    for pan_id in instruments:
        # Step 2: spectrometers for this instrument
        r = requests.get(f"{_BASE_URL}/files/{location}/{pan_id}", headers=_HEADERS)
        r.raise_for_status()
        spectrometers = [str(d["spectrometer"]) for d in r.json()]

        for spectrometer in spectrometers:
            # Step 3: check which processing levels are available for this spectrometer
            r = requests.get(
                f"{_BASE_URL}/files/{location}/{pan_id}/{spectrometer}", headers=_HEADERS
            )
            r.raise_for_status()
            available_levels = [d["level"] for d in r.json()]
            if level not in available_levels:
                continue

            # Step 4: files for the requested processing level and date range
            params = {"start": start, "end": end}
            if prod is not None:
                params["code"] = prod
            r = requests.get(
                f"{_BASE_URL}/files/{location}/{pan_id}/{spectrometer}/{level}",
                params=params,
                headers=_HEADERS,
            )
            if (
                r.status_code == 404
                and r.json().get("detail") == "No files found for the specified parameters"
            ):
                # API returns 404 instead of empty dataset when no files match
                warnings.warn(
                    f"No files found for {location}/{pan_id}/{spectrometer}/{level} "
                    f"in the specified date range ({start} to {end}).",
                    stacklevel=2,
                )
                continue
            elif r.status_code == 422 and r.json().get("detail") is not None:  # unprocessable
                # API returns 422 for invalid query, e.g. unexpected product code
                # Most likely a user error, but not necessarily
                msg = r.json()["detail"]
                raise RuntimeError(f"Got HTTP error 422 (unprocessable): {msg}")
            r.raise_for_status()
            for file_info in r.json():
                rows.append(
                    {
                        **file_info,
                        "location": location,
                        "pan_id": pan_id,
                        "spectrometer": spectrometer,
                        "level": level,
                    }
                )

    return pd.DataFrame(rows)


def download(dates, *, location=None, prod="rfuh5"):
    """Download PGN files for a location and date range.

    You can also use the data access portal to find and download the data you want:
    https://downloader.pandonia-global-network.org/

    Parameters
    ----------
    dates : datetime-like or array-like of datetime-like
        One date or an array; min and max are used as the inclusive time bounds
        (``start`` / ``end``) passed to the API.
    location : str or list of str, optional
        Location short name as returned by :func:`get_locations`,
        e.g. ``"Innsbruck"``.
        Default: all locations.
    prod : str or list of str, optional
        Product code filter (e.g. ``"rnvs3"``). Default: ``"rfuh5"``.

    Returns
    -------
    list of Path
        Paths to the downloaded files.
    """
    from itertools import product

    import requests

    if location is None:
        locations = get_locations().name
    elif isinstance(location, str):
        locations = [location]
    else:  # assume iterable of strings
        locations = location

    if isinstance(prod, str):
        prods = [prod]
    else:  # assume iterable of strings
        prods = prod

    paths = []
    for location, prod in product(locations, prods):
        files_df = get_location_files(location, dates, prod=prod)
        for row in files_df.itertuples():
            fn = row.filename
            url = f"{_BASE_URL}/download/{fn}"
            print(f"Downloading {fn}... ", end="", flush=True)
            r = requests.get(url, headers=_HEADERS, stream=True)
            r.raise_for_status()
            with open(fn, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
            print("done")
            paths.append(fn)

    return paths


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
    ``col02``, etc., with zero-padding width determined by the standard column
    count from *col_descs*.

    If the DataFrame has more columns than the standard count (i.e. optional
    layer columns are present), those extra columns are named
    ``col{N}lay{L}`` where *N* is the corresponding layer-1 column number and
    *L* is the layer index (starting at 2).

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe with integer column labels (0, 1, 2, ...).

    Returns
    -------
    pd.DataFrame
        Renamed dataframe with time as the index.
    """
    col_descs = df.attrs["_col_descs"]
    n = len(df.columns)
    n_std = sum(k.startswith("Column ") for k in col_descs)
    if not n_std:
        raise ValueError("No standard columns found in col_descs; cannot determine column naming.")
    width = len(str(n_std))
    rename_map = {0: "time", **{i: f"col{i+1:0{width}d}" for i in range(1, n_std)}}

    if n > n_std:
        from_col_key = next((k for k in col_descs if k.startswith("From Column")), None)
        if from_col_key:
            from_col_num = int(from_col_key.split()[2])
            m = re.search(r"(\d+) columns? per layer", col_descs[from_col_key])
            if m:
                stride = int(m.group(1))
                layer1_col_nums = list(range(from_col_num - stride, from_col_num))
                for extra_pos in range(n - n_std):
                    layer_num = extra_pos // stride + 2
                    in_layer_pos = extra_pos % stride
                    col_num = layer1_col_nums[in_layer_pos]
                    rename_map[n_std + extra_pos] = f"col{col_num:0{width}d}lay{layer_num}"

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
    n_std = sum(k.startswith("Column ") for k in col_descs)
    if not n_std and not include_optional_cols:
        warnings.warn(
            "No standard columns found in col_descs; including all columns.",
            stacklevel=2,
        )

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
            usecols=range(n_std),
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
    n_std = sum(k.startswith("Column ") for k in col_descs)
    width = len(str(n_std))

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
    std_cols = set()
    optional_desc = None
    for k, desc in col_descs.items():
        if k.startswith("From Column"):
            optional_desc = desc
        elif k.startswith("Column "):
            col_num = int(k.split()[1])
            col_name = f"col{col_num:0{width}d}"
            std_cols.add(col_name)
            if col_name in ds:
                ds[col_name].attrs["description"] = desc
    lay_cols = sorted(set(ds.keys()) - std_cols - {"latitude", "longitude", "altitude"})
    if lay_cols and optional_desc is not None:
        for k in lay_cols:
            ds[k].attrs["description"] = optional_desc

    ds = _maybe_add_layer_dim(ds)

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


def _maybe_add_layer_dim(ds):
    """Promote layer columns to ``(time, x, z)`` variables spanning all layers.

    Layer 1 data is already loaded in ``ds`` as ``(time, x)`` variables named
    ``col53``, ``col54``, etc.  Optional higher-layer data is present as
    ``col53lay2``, ``col54lay2``, ``col53lay3``, ... variables (named by
    :func:`_rename_and_format`).  This function merges each group into a single
    ``(time, x, z)`` variable (``z=0`` = layer 1) and drops the ``colXXlayY``
    variables from the dataset.

    Returns *ds* unchanged when no ``colXXlayY`` variables are present.

    Parameters
    ----------
    ds : xr.Dataset
        Dataset built from the standard columns by :func:`_df_to_ds`, possibly
        containing ``colXXlayY`` variables from the optional layer section.

    Returns
    -------
    xr.Dataset
        ``ds`` with the layer-1 ``(time, x)`` variables replaced by
        ``(time, x, z)`` variables covering all retrieved layers, and the
        intermediate ``colXXlayY`` variables removed.
    """
    lay_pattern = re.compile(r"^(col\d+)lay(\d+)$")

    # Group colXXlayY variables by their base column name
    layer_vars: dict[str, dict[int, str]] = {}
    for var in ds:
        m = lay_pattern.match(str(var))
        if m:
            base_col, layer_num = m.group(1), int(m.group(2))
            layer_vars.setdefault(base_col, {})[layer_num] = str(var)

    if not layer_vars:
        return ds

    lay_vars_to_drop = []
    for base_col, layers in layer_vars.items():
        if base_col not in ds:
            continue

        # Gather layer 1 (base var) then layers 2, 3, ... in order
        layer_arrays = [ds[base_col].values]  # (time, x=1)
        for layer_num in sorted(layers):
            lay_name = layers[layer_num]
            lay_vars_to_drop.append(lay_name)
            layer_arrays.append(ds[lay_name].values)

        # Stack along a new z axis: (time, x=1, total_layers)
        all_layers = np.stack(layer_arrays, axis=2)
        desc = re.sub(r" layer 1\b", " layer", ds[base_col].attrs.get("description", ""))
        ds[base_col] = (("time", "x", "z"), all_layers, {"description": desc})

    return ds.drop_vars(lay_vars_to_drop)


def open_dataset(file_path, *, layers=False):
    """Read a Pandora PGN file as an :class:`xr.Dataset`.

    Parameters
    ----------
    file_path : str or Path
        Path to a single Pandora PGN text file.
    layers : bool, optional
        If True, also parse the optional higher-layer results (profile data).
        Layer variables are merged into ``(time, x, z)`` arrays where ``z=0``
        is layer 1, using the ``colXXlayY`` intermediate naming produced by
        :func:`_rename_and_format`. Requires the slower Python CSV engine.
        Default False.

    Returns
    -------
    xr.Dataset
        Dataset from single file formatted for MELODIES MONET.
        If ``layers=True``, includes a ``z`` dimension for profile levels.
    """
    ds = _df_to_ds(_parse_file_to_df(file_path, include_optional_cols=layers))
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


def open_mfdataset(file_path, *, layers=False):
    """Opens multiple Pandora PGN files and combines them to
    MELODIES MONET compatible format.

    Parameters
    ----------
    file_path : str or list
        Path(s) to Pandora PGN text file(s).
    layers : bool, optional
        Passed to :func:`open_dataset`. If True, include optional higher-layer
        results as ``(time, x, z)`` variables. Default False.

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
    ds = open_dataset(files[0], layers=layers)
    if len(files) > 1:
        for f in files[1:]:
            ds2 = open_dataset(f, layers=layers)
            if ds.attrs["Data file version"] != ds2.attrs["Data file version"]:
                raise ValueError("Different data products and/or versions, cannot concatenate")
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
