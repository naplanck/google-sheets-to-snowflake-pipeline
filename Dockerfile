FROM public.ecr.aws/lambda/python:3.12

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/GS_Uploader.py ${LAMBDA_TASK_ROOT}/

CMD ["GS_Uploader.lambda_handler"]


