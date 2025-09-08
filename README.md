# Movies ETL Pipeline 🎬

A Python-based ETL pipeline that:

- **Extracts** data from a CSV file containing top movies
- **Transforms** data (runtime parsing, type conversions, data cleaning)
- **Loads** data into a PostgreSQL database
- **Analyzes** data with queries and pretty-printed results

---

## Features

- Easy-to-use command-line interface
- Custom SQL query support
- Modular and reusable Python code

---

## Requirements

- Python 3.8+
- PostgreSQL
- Python libraries: `pandas`, `psycopg2`, etc.

---

## Usage

```bash
# Load data from CSV into PostgreSQL
python movies_pipeline.py --file movies.csv        

# Run predefined analysis queries
python movies_pipeline.py --analyze                

# Run a custom SQL query
python movies_pipeline.py --custom "<SQL Query>"  
