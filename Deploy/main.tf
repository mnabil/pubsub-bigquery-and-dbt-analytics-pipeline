# -----------------------------------------------------------------------------
# Dataset
# -----------------------------------------------------------------------------
resource "google_bigquery_dataset" "raw" {
  dataset_id                 = "raw"
  location                   = "US"
  delete_contents_on_destroy = true
}

# -----------------------------------------------------------------------------
# Define pipelines
# -----------------------------------------------------------------------------
locals {
  pipelines = {
    clickstream = {
      topic   = "clickstream-events"
      dlq     = "clickstream-events-dlq"
      table   = "clickstream_events"
      schema  = "schemas/clickstream.json"
    }
    transaction = {
      topic   = "transaction-events"
      dlq     = "transaction-events-dlq"
      table   = "transaction_events"
      schema  = "schemas/transactions.json"
    }
    customer_support = {
      topic   = "customer-support-tickets"
      dlq     = "customer-support-tickets-dlq"
      table   = "customer_support_tickets"
      schema  = "schemas/customer_support_tickets.json"
    }
  }
}

# -----------------------------------------------------------------------------
# Fetch project number for Pub/Sub service account
# -----------------------------------------------------------------------------
data "google_project" "project" {
  project_id = var.project_id
}

locals {
  pubsub_sa = "service-${data.google_project.project.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

# -----------------------------------------------------------------------------
# Topics
# -----------------------------------------------------------------------------
resource "google_pubsub_topic" "topic" {
  for_each = local.pipelines
  name     = each.value.topic
  labels = {
    team        = "analytics"
    layer       = "bronze"
    environment = "dev"
  }
  message_retention_duration = "432000s"
}

resource "google_pubsub_topic" "dlq" {
  for_each = local.pipelines
  name     = each.value.dlq
  labels = {
    team        = "analytics"
    layer       = "dead-letter"
    environment = "dev"
  }
  message_retention_duration = "432000s"
}

# -----------------------------------------------------------------------------
# BigQuery tables
# -----------------------------------------------------------------------------
resource "google_bigquery_table" "table" {
  for_each  = local.pipelines
  dataset_id = google_bigquery_dataset.raw.dataset_id
  table_id   = each.value.table
  schema     = file(each.value.schema)
}

# -----------------------------------------------------------------------------
# Push subscriptions
# -----------------------------------------------------------------------------
resource "google_pubsub_subscription" "bq_sub" {
  for_each = local.pipelines
  name     = "${each.value.topic}-bigquery-subscription"
  topic    = google_pubsub_topic.topic[each.key].id

  bigquery_config {
    table            = "${google_bigquery_table.table[each.key].project}.${google_bigquery_table.table[each.key].dataset_id}.${google_bigquery_table.table[each.key].table_id}"
    use_table_schema = true
    write_metadata   = true
  }

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.dlq[each.key].id
    max_delivery_attempts = 5
  }

  ack_deadline_seconds       = 10
  expiration_policy { ttl    = "2678400s" }
  message_retention_duration = "432000s"
}

# -----------------------------------------------------------------------------
# Pull subscriptions
# -----------------------------------------------------------------------------
resource "google_pubsub_subscription" "pull_sub" {
  for_each = local.pipelines
  name     = "${each.value.topic}-pull-subscription"
  topic    = google_pubsub_topic.topic[each.key].id
  ack_deadline_seconds = 10
}

resource "google_pubsub_subscription" "dlq_pull_sub" {
  for_each = local.pipelines
  name     = "${each.value.dlq}-pull-subscription"
  topic    = google_pubsub_topic.dlq[each.key].id
  ack_deadline_seconds = 10
}

# -----------------------------------------------------------------------------
# IAM Bindings
# -----------------------------------------------------------------------------
# BigQuery Data Editor for Pub/Sub SA
resource "google_project_iam_member" "pubsub_bq_editor" {
  project = data.google_project.project.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${local.pubsub_sa}"
}

# Pub/Sub Publisher on DLQ topics
resource "google_pubsub_topic_iam_member" "dlq_publisher" {
  for_each = local.pipelines
  topic    = google_pubsub_topic.dlq[each.key].name
  role     = "roles/pubsub.publisher"
  member   = "serviceAccount:${local.pubsub_sa}"
}

# Pub/Sub Subscriber on push subscriptions
resource "google_pubsub_subscription_iam_member" "bq_subscriber" {
  for_each    = local.pipelines
  subscription = google_pubsub_subscription.bq_sub[each.key].name
  role         = "roles/pubsub.subscriber"
  member       = "serviceAccount:${local.pubsub_sa}"
}
