#  Africa Energy Data Pipeline

The **Africa Energy Data Pipeline** is an end-to-end ETL system that automates the collection, transformation, validation, and storage of African energy sector data from the [Africa Energy Portal](https://africa-energy-portal.org/). It standardizes datasets across all **54 African countries** for the years **2000–2022**, covering the subsectors: **Access**, **Supply**, and **Technical**.

![Africa Energy Data Map](images/africa-energy-data-map.png)


##  Overview

This project was developed as part of a data engineering internship task focused on building a **data engineering workflow** for large-scale energy datasets.  
The pipeline integrates cloud data extraction, validation, and cloud database storage using **MongoDB Atlas**, making the data ready for **visualization in Power BI or Tableau**.


##  Key Features

- Automated extraction of energy datasets from the Africa Energy Portal API  
- Data transformation and normalization across countries and subsectors  
- Validation checks for missing years, duplicates, and data consistency  
- Cloud storage using MongoDB Atlas  
- Output structured for direct visualization in BI tools  


##  Tech Stack

| Category | Tools |
|-----------|-------|
| **Language** | Python 3.12 |
| **Database** | MongoDB Atlas |
| **Libraries** | `requests`, `pandas`, `pymongo`, `json` |
| **IDE** | PyCharm |
| **Visualization (Optional)** | Power BI, Tableau |



## Project Structure
```
Africa_Energy_Data_Pipeline/
│
├── AFDP.py                           # Main ETL pipeline script
├── africa_energy_data.json           # Raw extracted data
├── formatted_africa_energy_data.json # Transformed and formatted data
├── validation_report.json            # Data validation report
├── .gitignore                        # Ignored files
└── README.md                         # Project documentation
```
## Setup Instructions

### How to Run

### 1️. Clone the Repository
git clone https://github.com/RonohSoy/Africa_Energy_Data_Pipeline.git

cd Africa_Energy_Data_Pipeline


### 2. Create a Virtual Environment

#### Create a virtual environment
python -m venv .venv

#### Activate the environment (Windows)
.venv\Scripts\activate

#### Install dependencies
pip install -r requirements.txt


### 3. Run the ETL Pipeline
python AFDP.py

## Output Files
| File                                | Description                                          |
| ----------------------------------- | ---------------------------------------------------- |
| `africa_energy_data.json`           | Raw data extracted from the portal                   |
| `formatted_africa_energy_data.json` | Cleaned and standardized data                        |
| `validation_report.json`            | Validation results (missing years, duplicates, etc.) |


## Validation Output Example

total_records: 1782  
missing_years: 0  
duplicates: 0  
countries_missing_subsectors: 0  


## Database Integration

The pipeline connects to MongoDB Atlas using pymongo:

client = pymongo.MongoClient("your_connection_string")

db = client["AfricaEnergyDB"]

collection = db["energy_data"]

##  Next Steps

- Implement automated data refresh using **Apache Airflow** or scheduled **cron jobs**  
- Integrate **renewable energy indicators** for deeper sector insights  
- Develop an interactive **Power BI dashboard** connected to **MongoDB Atlas** for real-time visualization  


## Author

**Peter Ronoh**  
Data Analyst & Data Engineer  
Nairobi, Kenya  
[GitHub Profile](https://github.com/RonohSoy)


##  License

This project is licensed under the **MIT License**.  
You are free to **use, modify, and distribute** this project with proper attribution.

