FROM public.ecr.aws/lambda/python:3.12

WORKDIR /app

COPY . /app

RUN pip install --no-cache-dir -r requirements.txt

CMD [ "lambda_function.lambda_handler" ]
