# Healthcare Analytics Data Warehouse

A production-grade analytics warehouse built with dbt and Snowflake, implementing dimensional modeling best practices for a healthcare enrollment support program.

## 🎯 Project Overview

This project demonstrates end-to-end data engineering skills including:
- **Layered dbt architecture** (staging → intermediate → marts)
- **Kimball dimensional modeling** (star schema with 3 dimensions, 2 facts)
- **Data quality testing** (62 tests covering uniqueness, relationships, accepted values)
- **Incremental loading** for performance optimization
- **Source freshness monitoring** for data reliability
- **Full documentation and lineage tracking**

## 📊 Business Context

Simulates a healthcare enrollment support program analyzing:
- Enrollment counts by region
- Plan churn rates
- Claims per member per month
- Average claim costs
- Support ticket volume trends

## 🏗️ Architecture

### Data Flow
```
Raw Data (Seeds)
    ↓
Staging Layer (4 models)
    ↓
Intermediate Layer (1 model)
    ↓
Marts Layer (6 models)
```

### Dimensional Model
- **Dimensions**: `dim_members`, `dim_plans`, `dim_regions`
- **Facts**: `fct_enrollments`, `fct_claims`
- **Analytical Mart**: `mart_member_monthly_metrics`

## 📁 Project Structure
```
models/
├── staging/
│   ├── sources.yml
│   ├── schema.yml
│   ├── stg_members.sql
│   ├── stg_plans.sql
│   ├── stg_enrollments.sql
│   └── stg_claims.sql
├── intermediate/
│   ├── schema.yml
│   └── int_member_enrollments.sql
└── marts/
    ├── schema.yml
    ├── dim_members.sql
    ├── dim_plans.sql
    ├── dim_regions.sql
    ├── fct_enrollments.sql
    ├── fct_claims.sql (incremental)
    └── mart_member_monthly_metrics.sql
```

## 🔑 Key Features

### 1. Staging Layer
- Standardizes column names and data types
- Applies basic data cleaning
- References source tables via `{{ source() }}` function

### 2. Intermediate Layer
- Joins members, enrollments, and plans
- Calculates enrollment duration
- Creates analytical flags (is_active, is_churned)

### 3. Marts Layer (Star Schema)
**Dimensions:**
- `dim_members`: Member demographics and signup info (SCD Type 1)
- `dim_plans`: Healthcare plan details with tier ordering
- `dim_regions`: Geographic region lookup

**Facts:**
- `fct_enrollments`: Enrollment events with premium calculations (Grain: one row per enrollment)
- `fct_claims`: Healthcare claims with date dimensions (Grain: one row per claim, incremental materialization)

**Analytical Mart:**
- `mart_member_monthly_metrics`: Aggregated member-level metrics for reporting

### 4. Data Quality
- **62 data tests** ensuring:
  - Primary key uniqueness and not-null constraints
  - Foreign key relationships between facts and dimensions
  - Accepted value constraints on categorical fields
  - Source data freshness monitoring

### 5. Performance Optimization
- Incremental loading strategy on `fct_claims` table
- Views for staging/intermediate, tables for marts
- Optimized for analytical query patterns

## 🚀 How to Run

### Prerequisites
- dbt Core installed (`pip install dbt-core dbt-snowflake`)
- Snowflake account with appropriate credentials
- Git

### Setup
```bash
# Clone repository
git clone https://github.com/YOUR_USERNAME/healthcare-analytics-dbt.git
cd healthcare-analytics-dbt/healthcare_analytics

# Configure profiles.yml with your Snowflake credentials
# (see dbt documentation)

# Install dependencies
dbt deps

# Load seed data
dbt seed

# Run all models
dbt run

# Run tests
dbt test

# Check source freshness
dbt source freshness

# Generate documentation
dbt docs generate
dbt docs serve
```

## 📈 Sample Analytics Queries

### Enrollment by Region
```sql
SELECT 
    region_code,
    COUNT(*) as total_enrollments,
    SUM(is_active) as active_enrollments,
    SUM(is_churned) as churned_enrollments,
    ROUND(SUM(is_churned) * 100.0 / COUNT(*), 2) as churn_rate_pct
FROM public_marts.fct_enrollments
GROUP BY region_code;
```

### Claim Analysis by Type
```sql
SELECT 
    claim_type,
    COUNT(*) as claim_count,
    ROUND(AVG(claim_amount), 2) as avg_claim_amount
FROM public_marts.fct_claims
GROUP BY claim_type
ORDER BY avg_claim_amount DESC;
```

### Member Utilization Metrics
```sql
SELECT 
    full_name,
    region,
    lifetime_claims,
    lifetime_claim_amount,
    avg_monthly_claims
FROM public_marts.mart_member_monthly_metrics
WHERE lifetime_claims > 0
ORDER BY lifetime_claim_amount DESC
LIMIT 10;
```

## 🛠️ Technical Decisions

### Why Kimball Dimensional Modeling?
- **Business user friendly**: Intuitive star schema for reporting tools
- **Query performance**: Optimized for analytical queries
- **Scalability**: Easy to add new dimensions or facts
- **Industry standard**: Widely understood by data teams

### Why Incremental Models?
- `fct_claims` uses incremental materialization to process only new claims
- Reduces compute time and costs for large datasets
- Maintains full history while optimizing build times

### Why This Layer Structure?
- **Staging**: Single source of truth, minimal transformations
- **Intermediate**: Reusable business logic, reduces duplication
- **Marts**: Analytics-ready datasets optimized for specific use cases

## 📊 Data Model Metrics
- **4 source tables** (50 members, 10 plans, 50 enrollments, 60 claims)
- **11 total models** (4 staging + 1 intermediate + 6 marts)
- **62 data quality tests**
- **3-layer architecture** following dbt best practices

## 🎓 Skills Demonstrated
- dbt Core development and best practices
- SQL (CTEs, window functions, aggregations, joins)
- Dimensional modeling (Kimball methodology)
- Data quality and testing strategies
- Incremental loading patterns
- Documentation and metadata management
- Version control with Git
- Snowflake data warehousing

## �� Project Context
Built as a portfolio project to demonstrate data engineering capabilities for healthcare analytics roles. Designed to showcase production-ready code, best practices, and business acumen in translating requirements into analytical data models.

## 📧 Contact
Thu (Ella) Ngo
Email: xuanthu.2612.40@gmail.com
LinkedIn: linkedin.com/in/thu-ella-ngo
GitHub: https://github.com/ellango2612
---

*Built with dbt, Snowflake, and SQL*
