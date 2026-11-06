FROM python:3.11-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    TZ=Asia/Shanghai

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY kaoguan.py .
COPY config.example.yaml ./config.yaml
COPY questions.example.yaml ./questions.yaml

# 年级词表：words/<年级>.txt，每行一个词
RUN mkdir -p /app/words

CMD ["python", "-u", "kaoguan.py"]
