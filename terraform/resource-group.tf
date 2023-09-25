resource "ibm_resource_group" "resource-group" {
    name       = "reputational-analysis"

    lifecycle {
        prevent_destroy = true
    }
}

output "resource_group" {
  value = ibm_resource_group.resource-group.name
}