FROM python:alpine
COPY terrible-horrible-ftp terrible-horrible-ftp
WORKDIR terrible-horrible-ftp
ENTRYPOINT ["python", "cli.py"]