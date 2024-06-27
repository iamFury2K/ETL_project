
---

# NYC Airbnb Data Analysis

This repository contains the code for an ETL pipeline that processes the NYC Airbnb Open Data dataset. The project leverages Metaflow for orchestration and pandas for data manipulation.

## Project Structure

```
.
├── AB_NYC_2019.csv          # The dataset containing NYC Airbnb data
├── load_data.py             # Script to load and preprocess the dataset
├── metaflow_ETL.py          # Metaflow pipeline for ETL
├── requirements.txt         # Required Python packages
└── README.md                # Project documentation
```

## Dataset

The dataset used in this project is `AB_NYC_2019.csv`, which contains detailed information about Airbnb listings in New York City. The data can be downloaded from [Kaggle's New York City Airbnb Open Data](https://www.kaggle.com/datasets/dgomonov/new-york-city-airbnb-open-data?resource=download).

## Getting Started
- The working demo of this project is here 
[Demo Video](https://drive.google.com/file/d/1bjucE9JlOwXdxtgPoYznlfwxRn20PJY-/view?usp=sharing)
### Prerequisites

Ensure you have Python 3.7+ and PostgreSQL installed on your system.

#### Python Setup

You can create a virtual environment to manage dependencies:

```bash
python -m venv venv
source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
```

#### PostgreSQL Setup

1. **Install PostgreSQL**:
   - On Ubuntu:
     ```bash
     sudo apt update
     sudo apt install postgresql postgresql-contrib
     ```
   - On macOS using Homebrew:
     ```bash
     brew install postgresql
     ```

2. **Start PostgreSQL Service**:
   - On Ubuntu:
     ```bash
     sudo service postgresql start
     ```
   - On macOS:
     ```bash
     brew services start postgresql
     ```

3. **Create a Database and User**:
   - Switch to the PostgreSQL user:
     ```bash
     sudo -i -u postgres
     ```
   - Open the PostgreSQL prompt:
     ```bash
     psql
     ```
   - Create a database:
     ```sql
     CREATE DATABASE airbnb_nyc;
     ```
   - Change user with a password:
     ```sql
     ALTER USER your_user WITH PASSWORD 'test';
     ```
     
   - Exit the PostgreSQL prompt:
     ```sql
     \q
     ```
   - Exit the PostgreSQL user:
     ```bash
     exit
     ```

### Installation

Install the required Python packages using the provided `requirements.txt` file:

```bash
pip install -r requirements.txt
```

### Running the ETL Pipeline

To run the ETL pipeline, execute the `metaflow_ETL.py` script:

```bash
python3 metaflow_ETL.py run
```

### Load and Preprocess Data

To load and preprocess the data, you can run the `load_data.py` script:

```bash
python3 load_data.py
```

## Project Components

### load_data.py

This script is responsible for loading and preprocessing the Airbnb dataset. It ensures the data is cleaned and ready for analysis.

### metaflow_ETL.py

This script defines a Metaflow pipeline for the ETL process. Metaflow is used to orchestrate the data flow and manage the various steps involved in the ETL process.

### requirements.txt

This file lists all the dependencies required for the project. Use this file to install the necessary packages.

## Dependencies

- astroid==3.2.2
- boto3==1.34.134
- botocore==1.34.134
- certifi==2024.6.2
- charset-normalizer==3.3.2
- dill==0.3.8
- greenlet==3.0.3
- idna==3.7
- isort==5.13.2
- jmespath==1.0.1
- mccabe==0.7.0
- metaflow==2.12.5
- numpy==2.0.0
- pandas==2.2.2
- platformdirs==4.2.2
- psycopg2-binary==2.9.9
- pylint==3.2.4
- python-dateutil==2.9.0.post0
- pytz==2024.1
- requests==2.32.3
- s3transfer==0.10.2
- six==1.16.0
- SQLAlchemy==2.0.31
- tomli==2.0.1
- tomlkit==0.12.5
- typing_extensions==4.12.2
- tzdata==2024.1
- urllib3==2.2.2

## Authors

- Muneeb Mushtaq Bhat

## License

This project is licensed under the MIT License.

## Acknowledgments

- Metaflow team for providing an excellent orchestration tool.
- Airbnb & Kaggle for providing the dataset.

---