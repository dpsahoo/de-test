FROM python:3.8

# Set environment variables
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

WORKDIR /app

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY ./app ./app

# CMD ["spark-submit", "--deploy-mode", "client", "--master", "local[*]",  --py-files . main.py]
CMD ["spark-submit", "--deploy-mode", "client", "--master", "local[*]",  "main.py"]