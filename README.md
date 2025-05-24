# Cubix Capstone - Data Engineering ETL Pipeline

A PySpark-based data engineering solution for sales analytics using **Databricks** and **Azure Data Lake Storage**. Implements medallion architecture (Bronze-Silver-Gold) for scalable data processing.

## 🏗️ Architecture & Concepts Applied

**Medallion Architecture:**
- **Bronze**: Raw data ingestion from source systems
- **Silver**: Cleaned and standardized data transformations  
- **Gold**: Business-ready aggregated metrics and analytics

**Data Engineering Concepts:**
- **SCD Type 1**: Slowly Changing Dimensions for master data updates
- **Delta Lake**: ACID transactions, schema evolution, time travel
- **Data Lake Pattern**: Structured storage in Azure Data Lake Gen2
- **ETL Pipeline**: Extract, Transform, Load with PySpark
- **Dimensional Modeling**: Star schema with fact and dimension tables

## 🛠️ Tech Stack

- **Databricks** (PySpark 3.5.5)
- **Delta Lake** (3.3.1) 
- **Azure Data Lake Storage Gen2**
- **Python 3.11+**
- **Poetry** (dependency management)

## 📁 Project Structure

```
cubix_capstone/
├── etl/
│   ├── bronze/
│   │   └── extract_and_load_file.py     # Raw data ingestion from source
│   ├── silver/                          # Data cleaning and standardization
│   │   ├── calendar.py                  # Calendar dimension transformation
│   │   ├── customers.py                 # Customer data cleaning & encoding
│   │   ├── products.py                  # Product catalog transformation
│   │   ├── product_category.py          # Product category standardization
│   │   ├── product_subcategory.py       # Product subcategory mapping
│   │   └── sales.py                     # Sales transaction processing
│   └── gold/                            # Business metrics and analytics
│       ├── wide_sales.py                # Denormalized sales table
│       ├── daily_sales_metrics.py       # Daily sales aggregations
│       └── daily_product_category_metrics.py  # Category performance
├── utils/
│   ├── authentication.py                # Azure AD authentication setup
│   ├── config.py                        # Configuration settings
│   ├── datalake.py                      # Data Lake read/write operations
│   └── scd.py                           # Slowly Changing Dimensions Type 1
├── tests/                               # Unit tests for all modules
│   ├── test_*.py                        # Comprehensive test coverage
│   └── conftest.py                      # Test configuration and fixtures
├── pyproject.toml                       # Poetry dependency management
└── poetry.lock                          # Locked dependency versions
```

## 🚀 Running on Databricks

### Databricks Setup

1. **Upload project to Databricks**
   ```bash
   # Clone repository to Databricks workspace
   git clone <your-repo-url>
   ```

2. **Install dependencies in Databricks**
   ```python
   # In Databricks notebook cell
   %pip install pyspark==3.5.5 delta-spark==3.3.1 numpy
   ```

3. **Configure Databricks cluster**
   - Runtime: DBR 13.3 LTS or higher
   - Python: 3.11+
   - Libraries: delta-spark, pyspark

4. **Set environment variables**
   ```python
   # In Databricks notebook
   import os
   os.environ["AZURE_TENANT_ID"] = "your-tenant-id"
   os.environ["AZURE_CLIENT_ID"] = "your-client-id"
   os.environ["AZURE_CLIENT_SECRET"] = "your-client-secret"
   ```

5. **Run ETL pipeline**
   ```python
   # Import and run your ETL modules
   from cubix_capstone.utils.authentication import authenticate_user
   from cubix_capstone.etl.bronze.extract_and_load_file import bronze_ingest
   
   authenticate_user()  # Setup Azure authentication
   # Run your pipeline...
   ```

### Prerequisites
- Databricks workspace
- Azure Data Lake Storage Gen2
- Azure AD service principal

### Setup

1. **Install dependencies using Poetry**
   ```bash
   # Install Poetry if not already installed
   curl -sSL https://install.python-poetry.org | python3 -
   
   # Install project dependencies
   poetry install
   
   # Activate virtual environment
   poetry shell
   ```

2. **Alternative: Using pip with requirements**
   ```bash
   # Generate requirements.txt from pyproject.toml if needed
   poetry export -f requirements.txt --output requirements.txt
   
   # Install with pip
   pip install -r requirements.txt
   ```

3. **Configure environment**
   ```bash
   export AZURE_TENANT_ID="your-tenant-id"
   export AZURE_CLIENT_ID="your-client-id" 
   export AZURE_CLIENT_SECRET="your-client-secret"
   ```

4. **Update config** in `utils/config.py`:
   ```python
   STORAGE_ACCOUNT_NAME = "your-storage-account"
   ```

## 📊 Usage Example

```python
# Bronze: Ingest raw data
from cubix_capstone.etl.bronze.extract_and_load_file import bronze_ingest

bronze_ingest(
    source_path="raw/sales",
    bronze_path="bronze/sales",
    file_name="sales_data.csv",
    container_name="data-container",
    partition_by=["year", "month"]
)

# Silver: Transform data
from cubix_capstone.etl.silver.customers import get_customers
customers_clean = get_customers(customers_raw)

# Gold: Generate business metrics
from cubix_capstone.etl.gold.daily_sales_metrics import get_daily_sales_metrics
daily_metrics = get_daily_sales_metrics(wide_sales)
```

## 🧪 Running Tests

### Local Testing
```bash
# Run all tests
poetry run pytest

# Run tests with verbose output
poetry run pytest -v

# Run specific test file
poetry run pytest tests/test_customers.py

# Run tests with coverage report
poetry run pytest --cov=cubix_capstone --cov-report=html

# Run tests for specific module
poetry run pytest tests/test_daily_sales_metrics.py -v
```

### Databricks Testing
```python
# In Databricks notebook
%pip install pytest

# Import test modules and run
import sys
sys.path.append('/Workspace/path/to/your/project')

# Run specific test
!python -m pytest tests/test_customers.py -v
```

## 📈 Key Features

- **Multi-source Integration**: Sales, customers, products, calendar data
- **Databricks Optimization**: Leverages Databricks for scalable processing
- **Delta Lake**: ACID transactions and time travel
- **SCD Type 1**: Master data updates
- **Business Metrics**: Daily sales and category performance analytics
- **Comprehensive Testing**: Full test coverage

## 🔐 Authentication

Uses Azure AD Service Principal for secure Data Lake access:

```python
from cubix_capstone.utils.authentication import authenticate_user
authenticate_user()  # Sets up Databricks authentication
```


## 📞 Contact

**Author**: phrubos (peter.hrubos.szte@gmail.com)

---
*Built for scalable data processing with Databricks & Azure*