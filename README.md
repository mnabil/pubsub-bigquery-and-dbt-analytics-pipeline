# Bitly Interview - Data Engineering Pipeline

This project demonstrates a complete data engineering pipeline using **dbt 2.0** with **BigQuery**, implementing modern data warehouse patterns including Kimball dimensional modeling, SCD Type 2, and data quality management.

## 🚀 Quick Start

### Prerequisites
- **Google Cloud Project** with billing enabled
- **Terraform** installed (version 1.0+)
- **dbt 2.0+** installed
- **Google Cloud** credentials configured

## ☁️ Infrastructure Setup (Terraform)

### 1. Configure GCP Project
```bash
# Update the project ID in Deploy/variables.tf
cd Deploy

# Edit variables.tf to set your project ID
variable "project_id" {
  default = "your-gcp-project-id"  # Change this to your project
}
```

### 2. Enable Required GCP APIs
```bash
# Enable necessary Google Cloud APIs
gcloud services enable cloudresourcemanager.googleapis.com --project=your-gcp-project-id
gcloud services enable bigquery.googleapis.com --project=your-gcp-project-id
gcloud services enable pubsub.googleapis.com --project=your-gcp-project-id
```

### 3. Deploy Infrastructure
```bash
cd Deploy

# Initialize Terraform
terraform init

# Review planned infrastructure changes
terraform plan

# Deploy BigQuery datasets, Pub/Sub topics, and data pipeline
terraform apply
```

### 4. Verify Infrastructure
The Terraform deployment creates:
- **BigQuery datasets**: `raw`, `staging`, `marts`
- **Pub/Sub topics**: `clickstream-events`, `transaction-events`, `customer-support-tickets`
- **Dead letter queues** for each topic
- **BigQuery tables** with appropriate schemas

## 🔧 Data Pipeline Setup (dbt)

### 1. Environment Setup
```bash
# Clone repository and navigate to dbt service
cd bitly/dbt_service

# Create and activate Python virtual environment
python3 -m venv bitly
source bitly/bin/activate

# Install dbt with BigQuery adapter
pip install dbt-core dbt-bigquery

# Configure profiles.yml for BigQuery connection
# (See dbt_project.yml for profile requirements)
```

### 2. Initial Data Load
```bash
# Load quarantine seed files (data quality filters)
dbt seed --select quarantined_events quarantined_sessions

# Load product catalog seed
dbt seed --select product_catalog
```

### 3. Run Complete Pipeline
```bash
# Build staging layer (clean + quarantine)
dbt run --models staging

# Build dimensional model + user analytics
dbt run --models marts

# Create SCD Type 2 snapshot
dbt snapshot
```

### 4. Data Quality Testing
```bash
# Run all tests
dbt test

# Test specific models
dbt test --models fct_events
dbt test --models staging
```

---

## 📊 Key Features Implemented

### 🔧 Data Engineering Patterns
- **Kimball Dimensional Modeling** - Star schema with fact/dimension tables
- **SCD Type 2** - Customer profile history tracking (manual + dbt snapshots)
- **Data Quality Management** - Comprehensive testing + quarantine pattern
- **Performance Optimization** - BigQuery partitioning, clustering, proper data types

### 📈 Business Analytics
- **RFM Analysis** - Customer segmentation (Recency, Frequency, Monetary)
- **User Journey Analytics** - Conversion funnel by category
- **Behavioral Segmentation** - Purchase patterns + browsing behavior
- **Daily Performance Metrics** - DAU, revenue, conversion rates

### 🛡️ Data Quality & Governance
- **Quarantine Pattern** - Filter bad data via seed files
- **Comprehensive Testing** - Schema tests + custom business logic validation
- **Data Lineage** - Pub/Sub metadata tracking for debugging
- **Documentation** - Complete schema documentation with data types

---

## 🎯 Model Descriptions

### Staging Layer
| Model | Purpose | Key Features |
|-------|---------|-------------|
| `stg_clickstream` | Clean event data | Quarantine filters, pub/sub metadata, flattened JSON |
| `stg_transactions` | Process transactions | Type casting, status mapping |
| `stg_customer_profiles` | Current customer state | Segmentation attributes for SCD2 |

### Marts Layer  
| Model | Purpose | Grain | Key Metrics |
|-------|---------|--------|------------|
| `fct_events` | Central fact table | Event-level | Revenue, event flags, product context |
| `fct_daily_summary` | Daily aggregates | Date + country | DAU, purchases, conversion rates |
| `fct_user_purchases` | Purchase analysis | User-level | RFM scores, customer segments |
| `fct_user_browsing` | Browse behavior | User-level | Page views, categories, cart adds |
| `dim_user_segments` | Customer 360 | User-level | Combined purchase + browse segments |

---

## 🧪 Testing Strategy

### Schema Tests (YAML-defined)
- **Null checks** on critical fields
- **Uniqueness** for primary keys  
- **Referential integrity** between fact/dimension tables
- **Data type validation**

### Custom Business Logic Tests (SQL files)
- `assert_fct_events_revenue_logic.sql` - Revenue only for purchases
- `assert_fct_events_revenue_positive.sql` - No negative revenue
- `assert_fct_events_single_event_type.sql` - Mutually exclusive event flags

### Data Quality Quarantine
- `quarantined_events.csv` - Block specific bad event IDs
- `quarantined_sessions.csv` - Block problematic session IDs
- Implemented with `NOT EXISTS` pattern for performance + null safety

---

## 🎯 Business Questions Answered

The dimensional model supports analysis of:

1. **Customer Lifecycle** - How do customers progress from browsers to VIP buyers?
2. **Product Performance** - Which categories drive highest conversion rates?
3. **Seasonal Patterns** - Weekend vs weekday behavior differences?
4. **User Segmentation** - RFM analysis for targeted marketing campaigns
5. **Data Quality Monitoring** - Which events are being quarantined and why?

---

## 📚 Analytics Queries

Sample business intelligence queries are provided in:
- `analytics_queries.sql` - Ready-to-run BigQuery SQL for business analysis
- Covers: customer segmentation, conversion funnels, time-series analysis, SCD2 history

---

## 🚀 Deployment Commands

### dbt Pipeline Commands
```bash
# Full pipeline refresh
dbt clean && dbt seed && dbt run && dbt test && dbt snapshot

# Production deployment (models only)
dbt run --target prod

# Incremental development
dbt run --models +fct_events+  # Run fct_events and all dependencies
dbt test --models marts        # Test marts layer only

# Debug failing tests
dbt test --select test_name:assert_fct_events_revenue_logic
```