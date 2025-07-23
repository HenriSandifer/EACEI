# EACEI - French Industrial Energy Consumption Analysis

**Author**: Henri Sandifer

**Status**: In Progress 🚧

---

## 1. Project Overview

This project aims to analyze the annual survey on energy consumption in French industry (_Enquête Annuelle sur la Consommation d'Énergie dans l'Industrie_ - **EACEI**), published by INSEE.  
The primary goal is to build a robust, end-to-end data pipeline to clean, transform, and model over a decade's worth of data (2010–2023).

The final output will be a clean, relational database and an interactive dashboard designed to explore trends in energy consumption across different industrial sectors, regions, and company sizes.

This project demonstrates skills in:

- Data cleaning
- ETL (Extract, Transform, Load)
- Database design
- Data visualization

---

## 2. Data Source

- **Provider**: INSEE (_Institut national de la statistique et des études économiques_)
- **Dataset**: EACEI
- **Years**: 2010–2023
- **Link**: [\[INSEE data source page (2023)\]](https://www.insee.fr/fr/statistiques/8566228?sommaire=8566231)

> The raw data consists of 156 individual CSV files, initially downloaded in .xls and .xlsx format from the individual EACEI pages for each year, with significant variations in structure, naming conventions, and categorical codes over the years.

---

## 3. Project Architecture

To handle the complexity and enable powerful analytics, this project uses a classic **Star Schema** database design.  
This separates the core measurements (facts) from their descriptive attributes (dimensions), leading to a clean, efficient, and scalable model.

- **Fact Table**: `faits_eacei`  
  → Contains all numerical values (consumption, purchases, prices, etc.)

- **Dimension Tables**:
  - `dim_naf`
  - `dim_region`
  - `dim_teff`
  - `dim_indicateur`
  - `dim_annee`

---

## 4. The Data Pipeline

The project is structured as a sequential pipeline, with scripts organized into phases:

### Phase 0 – Setup

- Initialization of the project structure and version control.

### Phase 1 – Cleaning & Standardization

- Process raw CSVs from `00_data_raw/`.
- Fuse headers, aggregate columns/rows to match modern conventions.
- Output tidy "wide" tables to `01_data_clean/`.

### Phase 2 – Dimension Generation

- Extract unique attributes.
- Generate primary keys.
- Output dimension tables to `03_database_final/`.

### Phase 3 – Fact Table Assembly

- Melt wide tables to long format.
- Join dimension table IDs to create `faits_eacei.csv`.

### Phase 4 – Visualization

- Load final tables into a BI tool for interactive dashboarding.

---

## 5. Tools & Technologies

- **Language**: Python 3.x
- **Libraries**: Pandas, NumPy
- **Database**: SQLite / PostgreSQL (planned)
- **Visualization**: Power BI / Tableau / Streamlit
- **Version Control**: Git & GitHub

---

## 6. Current Status & Next Steps

- [:heavy_check_mark:] Phase 1: Standardize all 156 raw CSV files
- [:heavy_check_mark:] Phase 2: Generate all dimension tables
- [:heavy_check_mark:] Phase 3: Build the final fact table
- [ ] Phase 4: Design and build the interactive dashboard
