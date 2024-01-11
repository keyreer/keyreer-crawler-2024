FROM amazon/aws-lambda-python:3.12

WORKDIR /app

COPY requirements.txt .

RUN python -m pip install --upgrade pip

COPY . .

CMD ["python", "jumpit_crawler.py"]
