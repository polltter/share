resource "ibm_code_engine_project" "code-engine-project" {
    name              = "rep-analysis-dev"
    resource_group_id = ibm_resource_group.resource-group.id

    lifecycle {
        prevent_destroy = true
    }

    timeouts {}
}

resource "ibm_code_engine_job" "queue" {
    image_reference               = var.docker_image
    image_secret                  = "cmore-container-registry"
    name                          = "worker"
    project_id                    = ibm_code_engine_project.code-engine-project.id
    run_mode                      = "daemon"

    lifecycle {
        prevent_destroy = true
    }
}

output "job_name" {
    value = ibm_code_engine_job.queue.name
    description = "The name of the job"
}

output "code_engine_project" {
  value = ibm_code_engine_project.code-engine-project.name
}