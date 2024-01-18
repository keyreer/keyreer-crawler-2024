import sys
import logging
import json
import boto3
import pymysql
from datetime import datetime
import os


# 한국 시간대로 설정
os.environ["TZ"] = "Asia/Seoul"

# rds settings
user_name = os.environ["USER_NAME"]
password = os.environ["PASSWORD"]
rds_host = os.environ["RDS_HOST"]
db_name = os.environ["DB_NAME"]
table_name = os.environ["TABLE_NAME"]

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# create the database connection outside of the handler to allow connections to be
# re-used by subsequent function invocations.
try:
    conn = pymysql.connect(
        host=rds_host, user=user_name, passwd=password, db=db_name, connect_timeout=5
    )
except pymysql.MySQLError as e:
    logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
    logger.error(e)
    sys.exit(1)

logger.info("SUCCESS: Connection to RDS for MySQL instance succeeded")


def lambda_handler(event, context):
    s3 = boto3.client("s3")
    print(s3)

    # 이벤트에서 버킷 이름과 파일 이름 가져오기
    bucket_name = event["detail"]["bucket"]["name"]
    file_name = event["detail"]["object"]["key"]
    logging.info(f"bucket_name: {bucket_name}, file_name: {file_name}")

    # S3에서 파일 내용을 읽어오기
    file_content = s3.get_object(Bucket=bucket_name, Key=file_name)
    file_text = file_content["Body"].read().decode("utf-8")
    data = json.loads(file_text)

    # MySQL 데이터베이스에 연결
    try:
        with conn.cursor() as cursor:
            # 현재 시간 구하기
            now = datetime.now().strftime("%Y-%m-%d")

            # 각 항목에 대해 SQL 쿼리 실행
            for item in data["result"]:
                sql = f"""INSERT IGNORE INTO {table_name} (platform, job_id, company, title, body, url, inserted_at)
                         VALUES (%s, %s, %s, %s, %s, %s, %s)"""
                cursor.execute(
                    sql,
                    (
                        item["platform"],
                        item["job_id"],
                        item["company"],
                        item["title"],
                        item["body"],
                        item["url"],
                        now,
                    ),
                )

        # 변경 사항 커밋
        conn.commit()
        logging.info("data insertion job done")
    finally:
        pass
    #     conn.close()

    return {"statusCode": 200, "body": json.dumps("Data processed successfully")}
