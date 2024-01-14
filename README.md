# Relation Between Housing Prices and the Ease of Obtaining a Construction Permit

This repository serves as an output for the course [MADE](https://oss.cs.fau.de/teaching/specific/made/) (Methods of Advanced Data Engineering) by the [Professorship for Open-Source Software](https://oss.cs.fau.de/) of [FAU](https://www.fau.eu/).

## Description

This project *Relation Between Housing Prices and the Ease of Obtaining a Construction Permit*, which explores datasets Doing Business from the World Bank and Analytical house price indicators from the statistical department of the OECD.

The project analyses relation between the Doing Business indicators and the Analytical house price indicators
Specifically the projects focuses on the possible correlation between the indicator Dealing with construction permits - Score of the Doing Business dataset and the indicator Nominal house price index of the Analytical house price indicators.

---

## Dependencies

1) Python 3.11+
2) Libraries defined in `project/requirements.txt`

---

## Goals

The primary purpose of the project is to build automated Data Engineering pipeline using Python.
It can be run by executing the shell file `project/pipeline.sh` after installing dependencies from `project/requirements.txt`.

The secondary purpose is to answer the project question: *Relation Between Housing Prices and the Ease of Obtaining a Construction Permit*.
The analysis is available in the final report accessible at [`project/report.ipynb`](project/report.ipynb).

### Data sources

#### Datasource1: The World Bank - Doing Business
* Metadata URL: https://datacatalog.worldbank.org/search/dataset/0038564/Doing-Business
* Data URL: https://databank.worldbank.org/data/download/DB_CSV.zip
* Data Type: CSV
* License: Creative Commons Attribution 4.0

The Doing Business project provides objective measures of business regulations and their enforcement across 190
economies. Economies are ranked on their ease of doing business, from 1–190. The rankings are determined by sorting
the aggregate scores (formerly called distance to frontier) on 10 topics, each consisting of several indicators,
giving equal weight to each topic.

World Bank (2023), "Doing Business", [https://datacatalog.worldbank.org/search/dataset/0038564/Doing-Business](https://datacatalog.worldbank.org/search/dataset/0038564/Doing-Business) (accessed on 8 November 2023). License: CC-BY 4.0

#### Datasource2: OECD.Stat - Analytical house price indicators
* Metadata URL: https://www.oecd-ilibrary.org/economics/data/prices/analytical-house-price-indicators_cbcc2905-en
* Data URL: https://stats.oecd.org/viewhtml.aspx?datasetcode=HOUSE_PRICES&lang=en
* Data Type: CSV
* License: Custom permissive *(You can extract from, download, copy, adapt, print, distribute, share and embed Data for any purpose, even for commercial use.)*

The dataset contains, in addition to nominal Residential Property Prices Indices (RPPIs), information on real house
prices, rental prices and the ratios of nominal prices to rents and to disposable household income per capita.

OECD (2023), "Prices: Analytical house price indicators", Main Economic Indicators (database), [https://doi.org/10.1787/cbcc2905-en](https://doi.org/10.1787/cbcc2905-en) (accessed on 8 November 2023).

---

## License

This project is licensed under the [CC BY 4.0](LICENSE). See the [LICENSE](LICENSE) file for details.

---

