# Finance Dashboard with SEC's Edgar API
Author: Ethan Chan

## Summary
The goal of this project is to create a simple and informative finance dashboard for public companies using financial data submitted to the SECs via 10-Qs and 10-Ks.
This project is still being developed on an ongoing basis and database will continue growing.

## Primary Python Libraries
Streamlit, Plotly, Polars, Pandas, SQLite3, and Requests modules.

## Data Ingestion/Cleaning
While the SEC API is well maintained, it is not very user friendly and building a data pipeline requires a custom API endpoint for each company.
For example, one company may report Cost of Revenue as "CostOfRevenue" and another company may report it as "CostOfGoodsSold."
The current process requires building specific ingestion and cleaning methods and then running a script to determine if the method will work for a select list of companies.

## Stock Data
Stock price data is taken from yFinance, an open source Python API for market data, but can be easily replaced with a third party provider.
