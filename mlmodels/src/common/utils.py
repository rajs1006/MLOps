import pandas as pd


def hourlySample(data):
    d = data.copy()
    d.index = pd.DatetimeIndex(d.index)
    return d.reindex(
        pd.date_range(start=d.index.min(), periods=24 * len(d), freq="h"),
        method="ffill",
    ).squeeze()


def findOutliers(df):
    s = df.copy()
    q1 = s.quantile(0.25)
    q3 = s.quantile(0.75)
    iqr = q3 - q1
    iqr_lower = q1 - 8 * iqr
    iqr_upper = q3 + 8 * iqr
    #     outliers = s[(s < iqr_lower) | (s > iqr_upper)]
    s[s < iqr_lower] = iqr_lower
    s[s > iqr_upper] = iqr_upper

    return s
