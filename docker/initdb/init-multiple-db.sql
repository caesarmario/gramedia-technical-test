-- ##############################################
-- Gramedia Digital - Data Engineer Take Home Test
-- SQL Init Script for PostgreSQL
-- Mario Caesar // caesarmario87@gmail.com
-- ##############################################

-- Create database for Apache Airflow
SELECT 'CREATE DATABASE airflow'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow') \gexec

-- Create database for dwh
SELECT 'CREATE DATABASE dwh'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'dwh') \gexec
