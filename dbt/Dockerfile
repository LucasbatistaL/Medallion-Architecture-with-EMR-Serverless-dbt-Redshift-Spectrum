FROM public.ecr.aws/docker/library/python:3.11-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc g++ && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir dbt-core dbt-athena-community

WORKDIR /dbt

COPY . .

RUN mkdir -p /root/.dbt && cp profiles.yml /root/.dbt/profiles.yml

HEALTHCHECK CMD ["dbt", "--version"]

ENTRYPOINT ["dbt"]
CMD ["run"]
