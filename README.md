# api-template-fastapi

## First thing to do
The things you have to do after creating a repository based on this template are as follows:
- Change `description` in `pyproject.toml` to the description of your API
- Change `repository` in `pyproject.toml` to the newly created repository
- Change `homepage` in `pyproject.toml` to your homepage

## How to build docker-image
```bash
$ docker-compose build

```

If you want to clone private repositories on GitHub while building the image,
use the following commands instead.
```bash
$ ssh-agent
$ ssh-add
$ export DOCKER_BUILDKIT=1
$ docker built -t api:latest . --ssh=default

```


## How to run the API server
Make sure to build the image first.
```bash
$ docker-compose up

```

You can update the behavior of your API by editing `api/server.py` while running the server
