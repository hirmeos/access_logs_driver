FROM python:2

RUN apt-get update && apt-get upgrade -y && \
    pip install --upgrade pip

WORKDIR /usr/src/app
RUN mkdir output
RUN mkdir cache
VOLUME ["/usr/src/app/output"]
VOLUME ["/usr/src/app/cache"]

COPY ./config/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt && \
    rm requirements.txt

COPY ./src/* ./

RUN flake8 --ignore=E221,E241 ./*

COPY ./config/spiders ./

CMD ["./run"]
