FROM python:3.4
WORKDIR /code
ADD requirements.txt /code/
RUN pip install -r requirements.txt
ADD . /code
# main.py takes two arguments, the duration of the streaming process and the number of words to be kept, before aggregating the rest into "other"
CMD python main.py 3 5
