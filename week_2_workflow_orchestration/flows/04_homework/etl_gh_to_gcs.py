from prefect.filesystems import GitHub

github_block = GitHub.load("de-zoomcamp")

github_block.get_directory(
    from_path = 'data-engineering-zoomcamp/week_2_workflow_orchestration',
    local_path = 'data-engineering-zoomcamp/week_2_workflow_orchestration/flows/04_homework/'
)