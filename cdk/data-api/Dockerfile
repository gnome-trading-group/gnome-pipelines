FROM public.ecr.aws/lambda/python:3.13

# The gnomepy that gnome-pipelines writes with (poetry.lock); keep them in step.
RUN pip install --no-cache-dir "gnomepy[live-data]==3.1.0"

COPY app.py ${LAMBDA_TASK_ROOT}/

CMD ["app.handler"]
