FROM python:latest
COPY . /python_app
WORKDIR /python_app

RUN pip3 install -r requirements.txt
CMD python ./short_term_main.py 