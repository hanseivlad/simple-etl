FROM ubuntu:16.04

RUN apt-get update
RUN apt-get install -y python-pip
RUN pip install awscli boto3

ADD ParsePatientRecords.py /

CMD ["/ParsePatientRecords.py"]
