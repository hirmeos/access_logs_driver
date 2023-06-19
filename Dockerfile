FROM python:3.7

WORKDIR /usr/src/app
RUN mkdir output cache

VOLUME ["/usr/src/app/output", "/usr/src/app/cache"]

COPY ./config/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt && \
    rm requirements.txt

COPY ./src/* ./

RUN flake8 ./*

COPY ./config/spiders ./

CMD ["./run"]
