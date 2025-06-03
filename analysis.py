import marimo

__generated_with = "0.13.15"
app = marimo.App(width="medium")


@app.cell
def _():
    import pathlib as pl

    import marimo as mo
    import pandas as pd
    import plotly.express as px

    DATA_C = pl.Path("data/collected")
    OUT = pl.Path("outputs/")
    return DATA_C, OUT, pd, pl, px


@app.cell
def _(DATA_C, OUT, pd, pl, px):
    def read_transfers(path: pl.Path) -> pd.DataFrame:
        """
        Read and clean raw transfers at the given path.
        Return a DataFrame with the columns

        - 'date'
        - 'day_of_week'
        - 'hour'
        - 'time_category'
        - 'day_type'
        - 'origin_route'
        - 'destination_route'
        - 'num_transfers'

        """
        return (
            pd.read_csv(path)
            .drop(["Unnamed: 0", "cal_year"], axis=1)
            .rename(
                columns={
                    "calendar_date": "date",
                    "time_display_hh24": "hour",
                    "journey_count": "num_transfers",
                    "cal_day_in_week": "day_of_week",
                }
            )
        )

    def plot_median_hourly_num_transfers(transfers: pd.DataFrame, title="") -> px.bar:
        median = (
            transfers.groupby(["date", "hour"])
            .sum(numeric_only=True)
            .groupby("hour")["num_transfers"]
            .median()
            .reset_index()
        )
        return px.bar(
            median,
            x="hour",
            y="num_transfers",
            title=title,
            labels={"hour": "Hour of day", "num_transfers": "Median #transfers"},
            template="plotly_white",
        )

    # Plot and save median hourly num transfers for our interchanges for March 2025
    for path in DATA_C.glob("*24hh.csv"):
        f = read_transfers(path).loc[
            lambda x: (x["date"] >= "2025-03-01") & (x["date"] <= "2025-03-31")
        ]
        interchange_name = " ".join(
            path.stem.replace("_transfers_24hh", "").split("_")
        ).title()
        title = f"{interchange_name} : Median hourly #transfers : March 2025"
        fig = plot_median_hourly_num_transfers(f, title)
        fig.show()
        fig.write_html(OUT / f"{interchange_name}.html", include_plotlyjs="cdn")

    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
