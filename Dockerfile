FROM python:3.8

# Install tools required by the OS
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      sudo \
      curl \
      vim \
      unzip \
      rsync \
      openjdk-11-jdk \
      build-essential \
      software-properties-common \
      ssh && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

WORKDIR /app

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY main.py /app

# CMD ["spark-submit", "--deploy-mode", "client", "--master", "local[*]",  --py-files . main.py]
CMD ["spark-submit", "--deploy-mode", "client", "--master", "local[*]",  "main.py"]