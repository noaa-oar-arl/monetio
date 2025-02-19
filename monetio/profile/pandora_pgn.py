import xarray as xr
import pandas as pd
import numpy as np
from glob import glob


def _parse_metadata(value):
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        pass
    try:
        return pd.to_datetime(value, format='ISO8601').to_datetime64()
    except ValueError:
        return value.lstrip().rstrip()


def _name_columns(index_number):
    return f"Column {index_number + 1}"


def _read_pandora_file(file_path):
    count_line_dividers = 0
    headers = {}
    global_attrs = {}
    data_collection = []
    with open(file_path, "r", encoding="latin-1") as f:
        for line in f:
            line_stripped = line.rstrip()
            if line_stripped.startswith("-----------"):
                count_line_dividers += 1
            elif count_line_dividers == 0:
                attr_name, value = tuple(line_stripped.split(":"))
                value = _parse_metadata(value)
                global_attrs[attr_name] = value
            elif count_line_dividers == 1:
                key, metadata = tuple(line_stripped.split(":"))
                headers[key] = metadata
            elif count_line_dividers == 2:
                data_collection.append(line_stripped.split())
    _df = pd.DataFrame(data_collection)
    times = pd.to_datetime(_df[0], format='ISO8601').dt.tz_localize(None)
    measurements = _df.loc[:, _df.columns != 0].apply(pd.to_numeric)
    df = pd.concat([times, measurements], axis=1)
    df = df.rename(_name_columns, axis="columns")
    df = df.rename(columns={'Column 1': 'time'})
    df = df.set_index('time')
    data = df.to_xarray().expand_dims("x", axis=1)
    data["latitude"] = (("x",), [global_attrs["Location latitude [deg]"]])
    data["longitude"] = (("x",), [global_attrs["Location longitude [deg]"]])
    data.attrs = global_attrs
    for k in headers:
        if k in data:
            data[k].attrs['description'] = headers[k]
        elif k.startswith('From Column'):
            optional_keys = headers[k]
    non_shared_keys = list(set(data.keys()) - set(headers.keys()))
    for k in non_shared_keys:
        data[k].attrs['description'] = optional_keys
    data["siteid"] = (("x",), [data.attrs["Short location name"]])
    data = data.assign_coords({"longitude": data["longitude"], "latitude": data["latitude"]})
    return data


def open_mfdataset(path):
    if isinstance(path, str):
        files = sorted(glob(path))
    ds = _read_pandora_file(files[0])
    if len(files) == 1:
        return ds
    for f in files[1:]:
        ds2 = _read_pandora_file(f)
        if ds['Data file version'] != ds2['Data file version']:
            raise Exception("Different data file versions, cannot concatenate")
        ds = xr.concat([ds, _read_pandora_file(f)], dim='x')
    return ds


if __name__ == "__main__":
    data = _read_pandora_file("Pandora204s1_BoulderCO-NCAR_L2_rfuh5p1-8.txt")
    data2 = _read_pandora_file("Pandora114s1_BuenosAires_L2_rfuh5p1-8.txt")

