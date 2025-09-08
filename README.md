# Movies ETL Pipeline

A Python-based ETL pipeline that:
- Extracts data from a CSV of top movies
- Transforms data (runtime parsing, type conversions)
- Loads into a PostgreSQL database
- Runs analysis queries with pretty-printed results

## Usage

```bash
python movies_pipeline.py --file movies.csv        # Load data
python movies_pipeline.py --analyze                # Run analysis
python movies_pipeline.py --custom "<SQL Query>"   # Custom SQL
