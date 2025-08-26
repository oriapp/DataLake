FROM jupyter/scipy-notebook:python-3.11

# Optional: set default token (compose can override)
ENV JUPYTER_TOKEN=dev

# Copy requirements into the image with proper permissions
USER root
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
RUN rm -f /tmp/requirements.txt
USER ${NB_UID}