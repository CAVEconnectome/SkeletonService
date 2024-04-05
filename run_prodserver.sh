#! /bin/bash 
export FLASK_APP=run.py
export FLASK_ENV=development
export SKELETONSERVICE_SETTINGS=$PWD/skeletonservice/instance/prod_config.py
export AUTH_URI=global.daf-apis.com/auth
export STICKY_AUTH_URL=global.daf-apis.com/sticky_auth
python run.py

