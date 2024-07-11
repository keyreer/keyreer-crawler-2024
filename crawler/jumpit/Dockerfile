FROM public.ecr.aws/lambda/python:3.11

COPY requirements.txt ${LAMBDA_TASK_ROOT}
# Datadog Lambda Library 설치
RUN pip install -r requirements.txt datadog-lambda

# Datadog Lambda Extension 설치
COPY --from=public.ecr.aws/datadog/lambda-extension:latest /opt/. /opt/

COPY src/* ${LAMBDA_TASK_ROOT}

# Datadog 환경 변수 설정
ENV DD_LAMBDA_HANDLER="main.handler"
ENV DD_TRACE_ENABLED="true"
ENV DD_SITE="datadoghq.com"
ENV DD_API_KEY_SECRET_ARN="arn:aws:secretsmanager:ap-northeast-2:992045752979:secret:DdApiKeySecret-uRPUw6o5zeRq-vLtF13"

# CMD [ "main.handler" ]
# Datadog 핸들러로 CMD 변경
CMD [ "datadog_lambda.handler.handler" ]

