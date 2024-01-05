import pandas as pd
from numpy import NaN


class IMPROVE:
    """IMPROVE -- Interagency Monitoring of Protected Visual Environments

    Primary website: http://vista.cira.colostate.edu/Improve/

    Data access (registration required): http://views.cira.colostate.edu/fed/DataWizard/Default.aspx

    Site metadata: http://vista.cira.colostate.edu/improve/wp-content/uploads/2022/09/IMPROVE_Sites_Active_and_Inactive_Updated_September_2022.xlsx
    """

    def __init__(self):
        self.datestr = []
        self.df = None
        self.daily = True

    def add_data(self, fname, add_meta=False, delimiter="\t"):  # TODO: `dates` arg?
        """Load IMPROVE data from CSV.

        .. note::
           This assumes that

           * You have downloaded the data from
             http://views.cira.colostate.edu/fed/DataWizard/Default.aspx
           * The data is the "IMPROVE Aerosol" dataset
           * Any number of sites
           * Parameters included are All
           * Fields include: Dataset, Site, Date, Parameter, POC, Data Value, Unit,
             Latitude, Longitude, State, EPA Site Code
             - Recommended: MDL
           * Options are delimited ',' data only and normalized skinny format

        Parameters
        ----------
        fname
            Path of file to load.

        Returns
        -------
        pandas.DataFrame
        """
        from .epa_util import read_monitor_file

        f = open(fname)
        lines = f.readlines()
        skiprows = 0
        skip = False
        for i, line in enumerate(lines):
            if line == "Data\n":
                skip = True
                skiprows = i + 1
                break
        # if meta data is included
        if skip:
            df = pd.read_csv(
                fname,
                delimiter=delimiter,
                parse_dates=[2],
                infer_datetime_format=True,
                dtype={"EPACode": str},
                skiprows=skiprows,
            )
        else:
            df = pd.read_csv(
                fname,
                delimiter=delimiter,
                parse_dates=[2],
                infer_datetime_format=True,
                dtype={"EPACode": str},
            )
        df.rename(columns={"EPACode": "epaid"}, inplace=True)
        df.rename(columns={"Val": "Obs"}, inplace=True)
        df.rename(columns={"State": "state_name"}, inplace=True)
        df.rename(columns={"ParamCode": "variable"}, inplace=True)
        df.rename(columns={"SiteCode": "siteid"}, inplace=True)
        df.rename(columns={"Unit": "Units"}, inplace=True)
        df.rename(columns={"Date": "time"}, inplace=True)
        df.drop("Dataset", axis=1, inplace=True)
        df["time"] = pd.to_datetime(df.time, format="%Y%m%d")
        df.columns = [i.lower() for i in df.columns]
        if pd.Series(df.keys()).isin(["epaid"]).max():
            df["epaid"] = df.epaid.astype(str).str.zfill(9)
        if add_meta:
            monitor_df = read_monitor_file(network="IMPROVE")  # .drop(
            # dropkeys, axis=1)
            df = df.merge(monitor_df, how="left", left_on="epaid", right_on="siteid")
            df.drop(["siteid_y", "state_name_y"], inplace=True, axis=1)
            df.rename(columns={"siteid_x": "siteid", "state_name_x": "state_name"}, inplace=True)

        try:
            df.obs.loc[df.obs < df.mdl] = NaN
        except Exception:
            df.obs.loc[df.obs < -900] = NaN
        self.df = df
        return df.copy()

    def load_hdf(self, fname, dates):  # TODO: unused
        self.df = pd.read_hdf(fname)
        self.get_date_range(self.dates)

    def get_date_range(self, dates):  # TODO: unused
        """Narrow dataset to requested datetime period."""
        self.dates = dates
        con = (self.df.time >= dates[0]) & (self.df.time <= dates[-1])
        self.df = self.df.loc[con]

    def set_daterange(self, begin="", end=""):  # TODO: unused
        """Set :attr:`dates` to a range of dates.

        Parameters
        ----------
        begin, end
            Passed to ``pd.date_range``.
        """
        dates = pd.date_range(start=begin, end=end, freq="H").values.astype("M8[s]").astype("O")
        self.dates = dates
