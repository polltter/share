terraform {
    backend "http" {
        address = "https://gitlab.com/api/v4/projects/33670736/terraform/state/reputational-analysis"
        lock_address = "https://gitlab.com/api/v4/projects/33670736/terraform/state/reputational-analysis/lock"
        unlock_address = "https://gitlab.com/api/v4/projects/33670736/terraform/state/reputational-analysis/lock"
        lock_method = "POST"
        unlock_method = "DELETE"
        retry_wait_min = 5
    }


    required_providers {
        ibm = {
            source = "IBM-Cloud/ibm"
            version = ">= 1.12.0"
        }
    }
}

provider "ibm" {
    ibmcloud_api_key = var.ibmcloud_api_key
    region = var.region
}