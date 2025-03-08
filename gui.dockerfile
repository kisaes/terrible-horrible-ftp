FROM python:alpine
COPY gui gui
WORKDIR gui
RUN pip install -r requirements.txt
ENTRYPOINT ["python", "main.py"]