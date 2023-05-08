FROM python:3.10

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

RUN apt-get update && pip install --upgrade pip && apt install --no-install-recommends gcc -y
COPY requirements.txt /app/requirements.txt
RUN pip install -r requirements.txt

COPY ./*.py /app/

# Запускаємо Python файл
CMD [ "python", "main.py" ]
