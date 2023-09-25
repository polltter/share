variable "ibmcloud_api_key" {
    description = "IBM Cloud API Key"
}

variable "region" {
    description = "IBM Cloud region"
    default = "eu-de"
}

variable "docker_image" {
    description = "Docker image to deploy"
}