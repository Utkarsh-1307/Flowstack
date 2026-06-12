FROM apache/spark:3.5.1

USER root
RUN pip install --no-cache-dir \
    pyarrow==16.0.0 \
    asyncpg==0.29.0 \
    python-dotenv==1.0.1

COPY apps/ /opt/spark/apps/
COPY core/ /opt/spark/core/

ENV PYTHONPATH=/opt/spark/core:$PYTHONPATH

# apache/spark image entrypoint handles master/worker mode via SPARK_MODE env
USER spark
