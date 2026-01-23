# search "terraform gg cloud provider"
terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "7.16.0"
    }
  }
}

provider "google" {
    # Add credential - Hard code - Should using GCloud SDK instead
    # export GOOGLE_CREDENTIAL='path to credential.json'
    # echo $GOOGLE_CREDENTIAL
    # terraform init

    credentials = "./keys/my-credentials.json"

    project     = "my-project-id"
    region      = "us-central1"
}

# Setting for GCP Cloud Storage - search "terraform gg cloud storage"
resource "google_storage_bucket" "auto-expire" {
  name          = "auto-expiring-bucket"
  location      = "US"
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 3 # day
    }
    action {
      type = "Delete" 
    }
  }

  lifecycle_rule {
    condition {
      age = 1 # day
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

# Can phai search Terraform gitignore de loai bo mot so cac config file nhay cam user, password...