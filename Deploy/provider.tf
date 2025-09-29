terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "6.8.0"
    }
  }
}

provider "google" {
  project = "sylvan-altar-384802"
  region  = "us-central1"
  credentials = file(var.gcp_svc_key)
  
}