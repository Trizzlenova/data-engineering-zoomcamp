# alternative to creating DockerContainer block in the UI

from prefect.infrastructure.docker import DockerContainer

docker_container_block = DockerContainer(
    image = '<username>/prefect:zoom',
    image_pull_policy = 'ALWAYS',
    auto_remove = True,
)

docker_container_block.save('de-prefect', overwrite = True)