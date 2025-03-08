# Project Overview

This repository contains three Python scripts designed for database performance analysis, SQL integration, and backend automation. Additionally, a standalone SQL script is included for direct execution without Python integration. The project also features an Entity-Relationship (ER) diagram for database design reference.

## Contents

### 1. Performance Analysis Script
- This script evaluates the time complexity and performance of various SQL queries.
- Helps in identifying bottlenecks and optimizing database queries for efficiency.

### 2. User Experience and SQL Integration Script
- A fully integrated Python application that connects with an SQL database.
- Implements table creation, triggers, functions, and stored procedures to manage data effectively.
- Provides a structured user interface for interacting with the database.

### 3. Backend Automation Script (BONUS TASK)
- Automates key backend operations using Celery.
- Manages asynchronous tasks such as background processing and scheduled database updates. (in this case Job scheduler )

### 4. Standalone SQL Script
- A bare SQL script that includes all essential database structures without Python integration.
- Contains table definitions, triggers, and stored procedures.
- Useful for executing database operations directly within an SQL environment.

## Entity-Relationship Diagram
- The ER diagram visualizes the database schema and relationships between entities.
- It serves as a reference for understanding the database structure.

## Usage
1. Run the **Performance Analysis Script** to evaluate query execution times.
2. Use the **SQL Integration Script** to interact with the database through Python.
3. Deploy the **Backend Automation Script** to manage asynchronous tasks efficiently.
4. Execute the **Standalone SQL Script** in a database management system to set up the schema manually.

## Requirements
- Python 3.x
- SQL Database (PostgreSQL)
- Celery (for backend automation)
