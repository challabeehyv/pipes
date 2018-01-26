# Setup Guide

Install pip requirements. It is recommended to do so in a virtualenv

```
pip install -r requirements.txt
```

Copy included airflow configuration into airflow directory. The setup assumes you have postgres running on port 5432. If you use a different port or db, you can change the connection string in the `sql_alchemy_conn` setting.

```
cp airflow.cfg ~/airflow/airflow.cfg
```

Replace all instances in `airflow.cfg` of `<user_home>` with full path to your home directory

Symlink dags to expected location

```
ln -s dags ~/airflow/dags
```

Symlink your hq virtualenv into your hq repo in a directory named `python_env` (same location as on production servers)

```
ln -s /path/to/virtualenv /path/to/cchq/python_env
```

create airflow dbs

```
airflow initdb
```

set airflow environment variables

```
airflow variables --set CCHQ_HOME /path/to/cchq
```

# How to reset

If your local copy gets messed up and you would like to start from a clean slate (somewhat likely during the development process), you can use the following command

```
airflow resetdb && airflow airflow variables --set CCHQ_HOME /path/to/cchq
```

It might be useful to create an alias for this. If you need to reset airflow state completely it is might also be necessary to delete batches from your hq `Batch` model that correspond to the dag you are trying to reset.
