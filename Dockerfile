FROM tiangolo/uwsgi-nginx-flask:python3.9

COPY requirements.txt /app/.
RUN python -m pip install --upgrade pip
# RUN pip install numpy
# RUN pip install scikit-build
RUN pip install -r requirements.txt
COPY . /app
COPY override/nginx.conf /etc/nginx/nginx.conf
COPY override/supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# ENV SKELETONSERVICE_SETTINGS /app/skeletonservice/instance/docker_cfg.py
ENV SKELETONSERVICE_SETTINGS /app/skeletonservice/instance/config.cfg

EXPOSE 5000
EXPOSE 80
