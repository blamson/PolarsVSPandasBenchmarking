import pandas as pd
import polars as pl
import time
import os


def main():
    # This is where we'll run all the trials I'll be setting up

    path = "data/Call_Data_20240521.csv"
    run_trials = {
        "read": False,
        "agg": True,
        "select": True,
        "filter": True
    }

    # Load Data and running load trials
    read_data_file = "read_data.csv"
    if run_trials["read"] or (not os.path.exists(f"data/{read_data_file}")):
        read_df, pd_df, pl_df = benchmark_loading(path, n_trials=10)
        read_df.write_csv(f"data/{read_data_file}")
    else:
        pd_df = pd.read_csv(path)
        pl_df = pl.read_csv(path)

    # Perform the rest of the aggregations
    agg_df = run_benchmark(
        benchmark_type="agg",
        run_trials=run_trials,
        benchmark_function=benchmark_aggregation,
        pd_df=pd_df, pl_df=pl_df, n_trials=100
    )

    select_df = run_benchmark(
        benchmark_type="select",
        run_trials=run_trials,
        benchmark_function=benchmark_selection,
        pd_df=pd_df, pl_df=pl_df, n_trials=100
    )

    filter_df = run_benchmark(
        benchmark_type="filter",
        run_trials=run_trials,
        benchmark_function=benchmark_filtering,
        pd_df=pd_df, pl_df=pl_df, n_trials=100
    )


def run_benchmark(benchmark_type, run_trials: dict, benchmark_function, pd_df, pl_df, n_trials):
    """
    Runs the benchmark, returns polar dataframe of benchmark and saves data to csv.

    :param benchmark_type: Benchmark to run. Filtering, selection, etc.
    :param run_trials: Dictionary specifying whether to run test or not
    :param benchmark_function: Function that actually runs the proper benchmark
    :param pd_df: Pandas dataframe
    :param pl_df: Polars dataframe
    :param n_trials: Number of trials
    :return: polars dataframe of results, or None if run_trials value is False.
    """

    df = None

    data_path = f"{benchmark_type}_data.csv"
    if run_trials[benchmark_type] or (not os.path.exists(f"data/{data_path}")):
        df = benchmark_function(pd_df, pl_df, n_trials)
        df.write_csv(f"data/{data_path}")

    return df

# I really could generalize a lot of this repetitive code, but it's more straightforward to copy-paste
# and collect the data than troubleshoot a more generalized solution.
def benchmark_loading(path, n_trials=10):
    """
    Runs both polars and pandas read_csv function over n trials and captures time data.
    :param path: Path to data
    :param n_trials: Number of trials
    :return: time duration dataframe, pandas dataframe, polars dataframe
    """

    duration_data = {
        "polars_read_duration": [],
        "pandas_read_duration": []
    }

    print("Beginning Loading Trials")
    trial_start_time = time.time()
    for i in range(n_trials):
        print(f"trial {i}")

        # Pandas trial
        start_time = time.time()
        pd_df = pd.read_csv(path)
        pd_end = time.time() - start_time
        duration_data["pandas_read_duration"].append(pd_end)

        # Polars trial
        start_time = time.time()
        pl_df = pl.read_csv(path)
        pl_end = time.time() - start_time
        duration_data["polars_read_duration"].append(pl_end)

    print(f"Trials Completed in {time.time() - trial_start_time} seconds")
    duration_df = pl.from_dict(duration_data)

    return duration_df, pd_df, pl_df


def benchmark_aggregation(pd_df, pl_df, n_trials=100):
    """
    Basic benchmark for running a simple group by and mean
    """

    duration_data = {
        "polars_agg_duration": [],
        "pandas_agg_duration": []
    }

    print("Beginning Aggregation Trials")
    trial_start_time = time.time()
    for i in range(n_trials):

        # Pandas trial
        start_time = time.time()
        pd_df.groupby("Call Type")["Priority"].mean()
        pd_end = time.time() - start_time
        duration_data["pandas_agg_duration"].append(pd_end)

        # Polars trial
        start_time = time.time()
        pl_df.group_by("Call Type").agg(pl.mean("Priority"))
        pl_end = time.time() - start_time
        duration_data["polars_agg_duration"].append(pl_end)

    print(f"Trials Completed in {time.time() - trial_start_time} seconds")
    duration_df = pl.from_dict(duration_data)

    return duration_df


def benchmark_selection(pd_df, pl_df, n_trials=100):
    """
    Simple test selecting 2 columns
    """
    duration_data = {
        "polars_select_duration": [],
        "pandas_select_duration": []
    }

    print("Beginning Selection Trials")
    trial_start_time = time.time()
    for i in range(n_trials):

        # Pandas trial
        start_time = time.time()
        pd_df[["Call Type", "Priority"]]
        pd_end = time.time() - start_time
        duration_data["pandas_select_duration"].append(pd_end)

        # Polars trial
        start_time = time.time()
        pl_df.select(["Call Type", "Priority"])
        pl_end = time.time() - start_time
        duration_data["polars_select_duration"].append(pl_end)

    print(f"Trials Completed in {time.time() - trial_start_time} seconds")
    duration_df = pl.from_dict(duration_data)

    return duration_df


def benchmark_filtering(pd_df, pl_df, n_trials=100):
    """
    Simple test selecting 2 columns
    """
    duration_data = {
        "polars_filtering_duration": [],
        "pandas_filtering_duration": []
    }

    print("Beginning Filtering Trials")
    trial_start_time = time.time()
    for i in range(n_trials):

        # Pandas trial
        start_time = time.time()
        pd_df.loc[pd_df["Priority"] == 4]
        pd_end = time.time() - start_time
        duration_data["pandas_filtering_duration"].append(pd_end)

        # Polars trial
        start_time = time.time()
        pl_df.filter(pl.col("Priority") == 4)
        pl_end = time.time() - start_time
        duration_data["polars_filtering_duration"].append(pl_end)

    print(f"Trials Completed in {time.time() - trial_start_time} seconds")
    duration_df = pl.from_dict(duration_data)

    return duration_df


if __name__ == "__main__":
    main()
