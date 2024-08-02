FROM ultralytics/yolov5:latest-cpu
WORKDIR /usr/src/app
RUN pip install --upgrade pip
COPY requirements.txt .
RUN pip install -r requirements.txt
RUN curl -L https://github.com/ultralytics/yolov5/releases/download/v6.1/yolov5s.pt -o yolov5s.pt
ENV SQS_QUEUE_NAME='https://sqs.us-east-1.amazonaws.com/933060838752/HasanQueue'
ENV BUCKET_NAME='hasan-abu-hasan-bucket'
COPY . .

CMD ["python3", "app.py"]
