FROM python:3.10

COPY . /themule

RUN pip install -e "/themule/[docker]"

COPY ./examples/app/ /app/

WORKDIR /app

ENV PYTHONUNBUFFERED=1

CMD ["python3", "/app/main.py"]
