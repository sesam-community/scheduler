FROM python:3-alpine
MAINTAINER Baard H. Rehn Johansen "baard.johansen@sesam.io"
COPY ./service /service
WORKDIR /service
RUN pip install -r requirements.txt
EXPOSE 5000/tcp
ENTRYPOINT ["python"]
CMD ["scheduler.py"]
