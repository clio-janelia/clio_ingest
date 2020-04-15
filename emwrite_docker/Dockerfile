ARG BASE=quay.io/pypa/manylinux2014_x86_64

FROM $BASE

LABEL description="A docker image for building portable Python linux binary wheels using modern GCC"
LABEL maintainer="jbms@google.com"

RUN yum -y update && yum -y install \
    python3 \
    python3-devel \
    devtoolset-9-gcc-c++ \
    && yum clean all

ENV PATH="/opt/rh/devtoolset-9/root/usr/bin:$PATH"

RUN python3 -m pip install tensorstore -vv

RUN python3 -m pip install numpy
RUN python3 -m pip install pillow
RUN python3 -m pip install gunicorn
RUN python3 -m pip install google-cloud-storage

COPY emwrite.py ./ 
CMD exec ${FLYEM_ENV}/bin/gunicorn --bind :$PORT --workers 1 --threads 1 emwrite:app 

