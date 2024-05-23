# PolarsVSPandasBenchmarking
A simple repository to compare the performance between polars and pandas to prove to myself that the swap is worth doing.

The data collected here were utilized for a fairly casual bit of statistical analysis. The full writeup of my findings is not on this repository and can instead be [found on my website here](https://bradylamson.com/p/polars-vs-pandas-benchmarking/)

# The Dataset Used

Quick overview of this data taken from the source: 

> This data represents police response activity. Each row is a record of a Call for Service (CfS) logged with the Seattle Police Department (SPD) Communications Center. Calls originated from the community and range from in progress or active emergencies to requests for problem solving. Additionally, officers will log calls from their observations of the field.

Data Being Used: [Link](https://data.seattle.gov/Public-Safety/Call-Data/33kz-ixgy/about_data)

The full dataset is huge is so obviously is not included in this repository, you will need to download this dataset from the source provided if you are curious to replicate my results.

Details of the data are a bit less important here as I'm using it entirely for benchmarking. Details in the link for the curious.

| Metric | Value |
|---|---|
| File Size | 1.17GB |
| Rows | 5.75 Million |
| Columns | 13 |

# Overview of Tests

I tested performance the following functionalities:

1. Data Reading
2. Data Filtering
3. Data Aggregation
4. Column Selection

For reading I collected 10 samples of data, for the rest of the tests I collected 100 samples.

# Repository Navigation

`run_trials.py` contains all of my benchmarking trials for data collection.

`notebooks/benchmarking.ipynb` is a sample writeup and visualization of the test results. 

The `data/` directory contains all of the data I collected from my trials.