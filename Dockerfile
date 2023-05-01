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
ENV ZIP_PASSWORD="<Enter the ZIP file password here>"

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

# Download the password-protected zip file from S3
RUN wget -O /app/test-data.zip "https://coding-challenge-public.s3.ap-southeast-2.amazonaws.com/test-data.zip"

# Unzip the file with the provided password
RUN unzip -P ${ZIP_PASSWORD} /app/test-data.zip -d /app

COPY main.py /app

# CMD ["spark-submit", "--deploy-mode", "client", "--master", "local[*]",  --py-files . main.py]
CMD ["spark-submit", "--deploy-mode", "client", "--master", "local[*]",  "main.py"]
