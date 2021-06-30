# api-meta-store

## How to build docker-image
```bash
$ docker-compose build

```


## How to run the API server
Make sure to build the image first.
```bash
$ docker-compose up

```


## List of Environment Varibales

- `PERMISSION_MANAGER_SERVICE`: Url of api-permission-manager service. 
- `API_IGNORE_PERMISSION_CHECK`: Whether to ignore checking permission via api-permission-manager, mainly for testing.
- `PORT`: Port to run server on.
- `PYDTK_META_DB_ENGINE`: Database engine to use for PyDTK.
- `PYDTK_META_DB_HOST`: Database host for PyDTK.
- `PYDTK_META_DB_USERNAME`: Username for PyDTK's MetaDBHandler.
- `PYDTK_META_DB_PASSWORD`: Passoword for PyDTK's MetaDBHandler.
- `PYDTK_META_DB_DATABASE`: Database name for PyDTK's MetaDBHandler.
- `API_DEBUG`: Enable debug mode if true.
