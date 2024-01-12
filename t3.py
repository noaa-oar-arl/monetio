"""
Testing loading GML ozonesondes
"""
# import re
from io import StringIO

import pandas as pd
import requests

# from tempfile import NamedTemporaryFile


# from monetio import icartt

# 100-m
url = r"https://gml.noaa.gov/aftp/data/ozwv/Ozonesonde/Boulder,%20Colorado/100%20Meter%20Average%20Files/bu1043_2023_12_27_17.l100"

r = requests.get(url)
r.raise_for_status()

# # ICARTT parser doesn't seem to work for it
# with NamedTemporaryFile(delete=False) as f:
#     f.write(r.content)
#     f.seek(0)
# ic = icartt.add_data(f.name)

blocks = r.text.replace("\r", "").split("\n\n")
assert len(blocks) == 5

# Metadata
meta = {}
todo = blocks[3].splitlines()
blah = ["Background: ", "Flowrate: ", "RH Corr: ", "Sonde Total O3 (SBUV): "]
for line in todo:
    key, val = line.split(":", 1)
    # maybes = re.split(r"\s{2,}", val.strip())
    # if len(maybes) == 1:
    #     meta[key] = val
    # else:
    #     meta[key] = maybes[0]
    #     todo.extend(maybes[1:])
    #     continue
    for key_ish in blah:
        if key_ish in val:
            i = val.index(key_ish)
            meta[key.strip()] = val[:i].strip()
            todo.append(val[i:])
            break
    else:
        meta[key.strip()] = val.strip()
    # TODO: replace multi space in val with single

col_info = [
    # name, units, na
    ("lev", "", None),
    ("press", "hPa", None),
    ("alt", "km", None),
    ("theta", "K", None),  # "Pottp", pretty sure this potential temperature
    ("temp", "degC", None),
    ("ftempv", "degC", "999.9"),  # TODO: what is?
    ("rh", "%", "999"),
    ("press_o3", "mPa", "99.90"),
    ("o3", "ppmv", "99.999"),
    ("o3_tot", "atm-cm", "99.9990"),  # 1 DU = 0.001 atm-cm
    ("pumptemp", "degC", "999.9"),  # "Ptemp", I think this is the pump temperature
    ("o3_num", "10^11 cm-3", "999.999"),
    ("o3_res", "DU", "9999"),
    ("o3_uncert", "%", "99999.000"),
]

assert len(col_info) == len(blocks[4].splitlines()[2].split()) == 14

names = [c[0] for c in col_info]
dtype = {c[0]: float for c in col_info}
dtype["lev"] = int
na_values = {c[0]: c[2] for c in col_info if c[2] is not None}

df = pd.read_csv(
    StringIO(blocks[4]),
    skiprows=2,
    header=None,
    delimiter=r"\s+",
    names=names,
    dtype=dtype,
    na_values=na_values,
)

theta_calc = (df.temp + 273.15) * (df.press / 1000) ** (-0.286)  # close to "Pottp"
