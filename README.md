# imdb_lakehouse_olap_pipeline
End-to-end data pipeline that processes the 2GB IMDb dataset through PySpark, stores as partitioned Parquet files, and serves sub-100ms    analytics via ClickHouse. Containerized with Podman Compose (Spark, ClickHouse, Prometheus, Grafana), tested with 45 automated tests, and    deployed via GitHub Actions CI/CD.
