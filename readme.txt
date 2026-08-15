Global Climate Volatility - Run Instructions

1. Start services:
   docker compose up -d

2. Activate the Python environment.

3. Install dependencies:
   pip install -r requirements.txt

4. Run the complete pipeline:
   ./run_pipeline.sh

5. PostgreSQL:
   Database: climate_db
   User: climate_user
   Host: localhost
   Port: 5432

6. MinIO:
   Endpoint: http://localhost:9000
   Bucket: climate-data

7. Power BI:
   Open dashboard/Global Climate Volatility Dashboard.pbix
   Refresh the PostgreSQL data source.