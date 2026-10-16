FROM public.ecr.aws/lambda/python:3.13

# The gnomepy that gnome-pipelines writes with (poetry.lock); keep them in step.
RUN pip install --no-cache-dir "gnomepy[live-data]==2.26.2"

COPY app.py ${LAMBDA_TASK_ROOT}/

CMD ["app.handler"]
