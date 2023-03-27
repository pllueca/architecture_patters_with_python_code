from python:3.11.2-alpine3.16


RUN apk add make
RUN pip install pytest sqlalchemy==2.0.7 ipdb

WORkDIR /app

COPY . .

CMD ["make", "test"]

