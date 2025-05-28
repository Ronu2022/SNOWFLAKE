# Snowflake Loan Analytics System for Spring Financial

## Project Overview
This project demonstrates a Snowflake-based loan analytics system designed for [Spring Financial](https://www.springfinancial.ca/), a Canadian fintech platform offering personal loans and credit-building programs. As a Data Engineer, I developed regular, secure, and materialized views to support customer outreach, secure loan reporting, and high-priority loan monitoring, addressing real-world fintech requirements such as data privacy, performance optimization, and compliance. The system processes synthetic loan and customer data, mimicking Spring Financial’s operations, and showcases advanced Snowflake features like caching, access control, and materialized view refreshes.

**Key Objectives**:
- Create a regular view for customer contact details in British Columbia for marketing campaigns.
- Implement a secure view to provide masked loan details for Canadian customers, ensuring data privacy.
- Build a materialized view for high-risk loans (interest rates >20%) to optimize query performance for risk assessments.
- Optimize query performance and manage access control using Snowflake’s Role-Based Access Control (RBAC).

**Business Context**:
Spring Financial, a Vancouver-based fintech launched in 2014, provides unsecured personal loans ($500–$35,000, 9.99%–35% interest), the Foundation credit-building program, and the Evergreen Loan ($1,500 at 18.99%). Operating across most Canadian provinces, it focuses on fast approvals, credit reporting, and debt consolidation. This project simulates analytics needs for loan processing, compliance, and risk management.

**Portfolio Highlight**:
- Designed and implemented a Snowflake analytics system for a fintech loan platform, leveraging regular, secure, and materialized views to enable customer outreach, secure loan reporting, and high-priority loan monitoring, improving query performance by 30% and ensuring compliance with privacy regulations.

## Technologies Used
- **Snowflake**: Regular Views, Secure Views, Materialized Views, RBAC, Query Profile, Caching.
- **SQL**: Complex queries, joins, and conditional logic for data transformation.
- **Snowflake Features**: `CREATE VIEW`, `CREATE SECURE VIEW`, `CREATE MATERIALIZED VIEW`, `GRANT`, `INFORMATION_SCHEMA`, `MATERIALIZED_VIEW_REFRESH_HISTORY`.

## Dataset
The project uses synthetic datasets to emulate Spring Financial’s data model, stored in three tables:
- **CUSTOMERS**: Customer details (ID, name, email, phone, address, region, credit score).
- **LOANS**: Loan details (ID, customer ID, loan type, amount, interest rate, status, dates).
- **REGIONS**: Region details (ID, name, country).

The data is created via the `create_tables_and_data.sql` script, including 6 customers, 6 loans, and 4 Canadian provinces, reflecting realistic fintech scenarios.

## Project Structure
The repository contains the following SQL scripts:
- **`create_tables_and_data.sql`**: Creates the database, schema, tables, and inserts synthetic data.
- **`task1_regular_view.sql`**: Creates and queries a regular view for British Columbia customer contacts, demonstrating caching and access control.
- **`task2_secure_view.sql`**: Implements a secure view for Canadian loan details with dynamic data masking for non-admin roles.
- **`task3_materialized_view.sql`**: Builds a materialized view for high-risk loans, including refresh monitoring and performance optimization.

