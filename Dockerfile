FROM python:3.8

WORKDIR ./Bank_internal_system

ADD . .

RUN pip install -r requirements.txt

CMD ["python", "./main.py"]