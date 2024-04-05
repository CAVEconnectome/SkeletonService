# SkeletonService

## Getting started
Install docker.

Create the instance/ directory under skeletonservice/ and create config.cfg inside that new directory.

Edit skeletonservice/instance/config.cfg (see config.cfg.example) to include your deployment specific secret keys and neuroglancer server

```
  # spin up a postgres database on your localhost
  docker-compose up -d
  pip install -r requirements.txt
  python setup.py install
  ./run_devserver.sh
```

navigate to http://localhost:5000/admin for admin interface

