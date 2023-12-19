import functions_framework
from google.cloud import dataproc_v1 as dataproc
from google.cloud import storage

# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def run_dataproc_job(cloud_event):
    data = cloud_event.data
    # Extract information about the new file
    bucket_name = data['bucket']
    file_name = data['name']

    # Specify your Dataproc job details
    project_id = 'airbnb-data-analytics-project'
    region = 'us-central1'
    cluster_name = 'airbnb-cluster'
    job_file_uri = 'gs://yash_airbnb_raw_data/Airbnb.py'

    # Create a Dataproc job
    job = {
        'reference': {'project_id': project_id},
        'placement': {'cluster_name': cluster_name},
        'pyspark_job': {'main_python_file_uri': job_file_uri},
    }

    # Submit the Dataproc job
    dataproc_client = dataproc.ClusterControllerClient()
    operation = dataproc_client.submit_job_as_operation(
        request={'project_id': project_id, 'region': region, 'job': job}
    )
    operation.result()


# This Cloud Function will be triggered when a new file is added to the specified GCS bucket.

