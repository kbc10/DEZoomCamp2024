FROM python:3.10

RUN pip install pandas

WORKDiR /app

COPY pipeline.py pipeline.py

ENTRYPOINT ["python", "pipeline.py"]
