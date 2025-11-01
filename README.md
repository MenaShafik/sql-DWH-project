🎉 Welcome to #sql-DWH-project
“SQL Server Data Warehouse Project” — the mission is to design and build a modern,
scalable, and efficient Data Warehouse (DWH) using Microsoft SQL Server.
---
The project aims to build a data warehouse for a related ERP &
 & CRM System
_____________________________________
•	Project Requirements 
1.	Objective
Develop a modern data warehousing using SQL server to
Consolidate sales data, enabling analytical reporting
And informed decision-making.
2.	Specifications
Data Sources: Import data from two source systems (ERP and CRM) provided as CSV files.
Data Quality: Cleanse and resolve data quality issues prior to analysis.
Integration: Combine both sources into a single,
user-friendly data model designed for analytical queries.
Scope: Focus on the latest dataset only,
data historiography is not required.
Documentation: Provide clear documentation of the data model to support both business stakeholders and analytics.
________________________________________
ETL pipeline
Extraction:
in this project I used full extraction method
With File Parsing Technique 
Transformation: 
Data enrichment – Data Integration – Derived Columns - Data Normalization & Standardization – Business Rules & Logic – Data Aggregations.

Data Cleansing: 
Remove Duplicates – Data Filtering – Handling Missing Data – Handling Invalid Values – Handling Unwanted Spaces – Data type Casting – Outlier Detection.

Load: 
Batch processing – Full Load (Truncate & Insert) – SCD 1
(Slowly Changing Dimension) with overwrite access

Data Architecture:
In This Project I Used a Medallion Architecture
As a Three Layers: Bronze – Silver – Gold
<img width="1067" height="558" alt="image" src="https://github.com/user-attachments/assets/91e8ddee-75be-488b-9e31-41dd2fbf2f4e" />

