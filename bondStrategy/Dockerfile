FROM python:3.9.6-slim-buster
WORKDIR /usr/src/bond

ENV TZ=Asia/Seoul

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

CMD ["python3", "telegram_bot.py"]