import marimo

__generated_with = "0.13.15"
app = marimo.App(width="medium")


@app.cell
def _():
    import pathlib as pl

    import marimo as mo
    import pandas as pd
    import plotly.express as px
    from loguru import logger

    DATA_C = pl.Path("data/collected")
    OUT = pl.Path("outputs/")
    return DATA_C, OUT, logger, pd, pl, px


@app.cell
def _(pd, pl, px):
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
                    "day_type": "is_weekday",
                }
            )
            .assign(
                is_weekday=lambda x: x["is_weekday"].str.lower() == "weekday",
                date=lambda x: pd.to_datetime(x["date"]),
            )
        )

    def plot_median_num_transfers_by_hour(transfers: pd.DataFrame, title="") -> px.bar:
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

    def plot_median_num_transfers_by_day_of_week(
        transfers: pd.DataFrame, title=""
    ) -> px.bar:
        f = (
            transfers.groupby(["date", "day_of_week"])
            .sum(numeric_only=True)
            .groupby("day_of_week")["num_transfers"]
            .median()
            .reset_index()
        )
        days_order = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
        return px.bar(
            f,
            x="day_of_week",
            y="num_transfers",
            title=title,
            labels={"day_of_week": "Day of week", "num_transfers": "Median #transfers"},
            category_orders={"day_of_week": days_order},
            template="plotly_white",
        )

    def split_by_month(
        transfers: pd.DataFrame, months: list[str]
    ) -> dict[str, pd.DataFrame]:
        """
        Split the given the table of transfers into monthly datasets for the given months
        expressed as YYYYMM date strings.
        Return a dictionary of the form month -> transfers during month.
        """
        return {
            month: transfers.loc[lambda x: x["date"].dt.strftime("%Y%m") == month]
            for month in months
        }

    def compare(transfers: pd.DataFrame, two_months: list[str]) -> pd.DataFrame:
        months = sorted(two_months)
        t_by_month = split_by_month(transfers, months)
        ts = t_by_month.values()
        f = pd.DataFrame(
            data={
                "month": months,
                "num_transfers_total": [t["num_transfers"].sum() for t in ts],
                "num_transfers_daily_avg": [
                    t["num_transfers"].sum() / t["date"].nunique() for t in ts
                ],
            }
        )
        f["diff_total"] = f["num_transfers_total"].diff()
        f["pc_change_total"] = 100 * f["diff_total"] / f["num_transfers_total"].iat[0]
        f["diff_daily_avg"] = f["num_transfers_daily_avg"].diff()
        f["pc_change_daily_avg"] = (
            100 * f["diff_daily_avg"] / f["num_transfers_daily_avg"].iat[0]
        )
        return f

    def plot_median_num_transfers_by_hour_grouped(
        t1: pd.DataFrame, t2: pd.DataFrame, title=""
    ) -> px.bar:
        def get_median_by_hour(transfers):
            f = (
                transfers.groupby(["date", "hour"])
                .sum(numeric_only=True)
                .groupby("hour")["num_transfers"]
                .median()
                .reset_index()
            )
            f["month"] = transfers["date"].iat[0].strftime("%Y-%m")
            return f

        f = pd.concat([get_median_by_hour(t) for t in [t1, t2]], ignore_index=True)
        return px.bar(
            f,
            x="hour",
            y="num_transfers",
            color="month",
            barmode="group",
            title=title,
            labels={
                "hour": "Hour of day",
                "num_transfers": "Median #transfers",
                "year": "Year",
            },
            template="plotly_white",
        )

    return (
        compare,
        plot_median_num_transfers_by_day_of_week,
        plot_median_num_transfers_by_hour_grouped,
        read_transfers,
        split_by_month,
    )


@app.cell
def _(
    DATA_C,
    OUT,
    compare,
    logger,
    plot_median_num_transfers_by_day_of_week,
    plot_median_num_transfers_by_hour_grouped,
    read_transfers,
    split_by_month,
):
    # Plot and save median hourly num transfers for our interchanges for March 2025
    months = ["202403", "202503"]
    for path in sorted(DATA_C.glob("*_transfers.csv")):
        # Get interchange name from path
        stem = path.stem.replace("_transfers", "")
        name = " ".join(stem.split("_")).title()

        # Load transfers and split by given months
        transfers = read_transfers(path)
        f = compare(transfers, months)
        logger.info(name)
        print(f)

        # Plot
        title = f"{name} : Median #transfers by hour"
        t_by_month = split_by_month(transfers, months)
        fig = plot_median_num_transfers_by_hour_grouped(*t_by_month.values(), title=title)
        fig.show()
        fig.write_html(
            OUT / f"{stem}_median_num_transfers_by_hour_chart.html",
            include_plotlyjs="cdn",
        )

        # Plot
        title = f"{name} : Median #transfers by day of week :: 2025-03"
        fig = plot_median_num_transfers_by_day_of_week(t_by_month["202503"], title=title)
        fig.show()
        fig.write_html(
            OUT / f"{stem}_median_num_transfers_by_day_of_week_chart_202503.html",
            include_plotlyjs="cdn",
        )

    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
