# 표준 라이브러리
from datetime import date, datetime
import json
import os
import sys
import random
import time
import math
import logging
from typing import List, Dict

# 서드파티 라이브러리
import asyncio
import aiohttp
import boto3


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],  # 표준 출력으로 출력 설정
)


class Scraper:
    def __init__(self):
        self.job_list = []
        self.user_agent_list = [
            "Mozilla/5.0 (iPad; CPU OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.83 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36",
        ]
        self.headers = {"User-Agent": random.choice(self.user_agent_list)}

    async def fetch_all_positions_detail(self) -> List[Dict]:
        """
        id를 통해 모든 공고의 상세 정보를 수집
        """
        start_time = time.time()
        async with aiohttp.ClientSession() as session:
            position_ids = await self.fetch_all_position_ids(session)

            tasks = []
            for i, position_id in enumerate(position_ids):
                task = asyncio.create_task(
                    self.fetch_position_detail(session, position_id)
                )
                tasks.append(task)
                if i % 100 == 0 or i == len(position_ids) - 1:  # 100개씩 묶어서 요청
                    await asyncio.gather(*tasks)
                    logging.info(
                        f"scraped {i} data - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                    )
                    tasks = []
                    await asyncio.sleep(1.5)

        end_time = time.time()
        logging.info(f"elapsed time: {end_time - start_time}")
        return

    async def fetch_pages_count(self, session) -> int:
        """
        전체 공고의 페이지 수를 반환 (점핏은 한 페이지당 16개의 공고를 반환)
        """
        url = "https://api.jumpit.co.kr/api/positions"
        async with session.get(url, headers=self.headers) as response:
            json_data = await response.json()
            total_count = json_data["result"]["totalCount"]
            return math.ceil(total_count / 16)

    async def fetch_ids_from_page(self, session, page: int) -> List[int]:
        """
        한 페이지에 있는 공고들의 id 리스트를 반환
        """
        url = f"https://api.jumpit.co.kr/api/positions?page={page}"
        async with session.get(url, headers=self.headers) as response:
            try:
                json_data = await response.json()
                positions = json_data["result"]["positions"]
                return [position["id"] for position in positions]
            except:
                return []

    async def fetch_all_position_ids(self, session) -> List[int]:
        """
        모든 공고들의 id 리스트를 반환
        """
        page_count = await self.fetch_pages_count(session)

        tasks = [
            self.fetch_ids_from_page(session, page) for page in range(1, page_count + 1)
        ]
        pages_position_ids = await asyncio.gather(*tasks)

        # Flatten the list of lists
        position_ids = []
        for page_position_ids in pages_position_ids:
            position_ids.extend(page_position_ids)

        return position_ids

    async def fetch_position_detail(self, session, position_id: int) -> Dict:
        """
        공고 상세 정보를 반환
        """
        url = f"https://api.jumpit.co.kr/api/position/{position_id}"
        async with session.get(url, headers=self.headers) as response:
            if response.status != 200:
                logging.error(f"Error: {response.status} - {url}")
                return
            response_json = await response.json()

        result = response_json["result"]
        position_dict = dict()

        position_dict["platform"] = "jumpit"
        position_dict["job_id"] = position_id
        position_dict["company"] = result["companyName"]
        position_dict["title"] = result["title"]
        position_dict["body"] = "\n\n".join(
            [
                result["preferredRequirements"],
                result["qualifications"],
                result["responsibility"],
            ]
        )
        position_dict["url"] = f"https://www.jumpit.co.kr/position/{position_id}"

        self.job_list.append(position_dict)
        return

    @staticmethod
    def save_to_json(data_list: list):
        folder = "jumpit_data"
        filename = "jumpit.json"
        json_data = {"result": data_list}

        if not os.path.exists(folder):
            os.makedirs(folder)

        with open(os.path.join(folder, filename), "w", encoding="utf-8") as json_file:
            json.dump(json_data, json_file, ensure_ascii=False, indent=4)
            print(f"Data saved to {filename}")

    # @staticmethod
    # def upload_to_s3(
    #     file_path: str,
    #     bucket_name: str,
    #     access_key: str,
    #     secret_key: str,
    #     region_name: str,
    # ) -> None:
    #     """Uploads the specified file to an AWS S3 bucket."""

    #     print("Start upload!")

    #     today = date.today()
    #     year = str(today.year)
    #     month = str(today.month).zfill(2)
    #     day = str(today.day).zfill(2)
    #     FILE_NAME = f"jumpit/year={year}/month={month}/day={day}/jumpit.json"

    #     s3 = boto3.client(
    #         "s3",
    #         aws_access_key_id=access_key,
    #         aws_secret_access_key=secret_key,
    #         region_name=region_name,
    #     )

    #     s3.upload_file(file_path, bucket_name, FILE_NAME)

    #     path_name = os.path.join(bucket_name, FILE_NAME)
    #     print(f"End Upload to s3://{path_name}")


async def main():
    scraper = Scraper()
    await scraper.fetch_all_positions_detail()
    Scraper.save_to_json(scraper.job_list)


if __name__ == "__main__":
    # main()
    asyncio.run(main())
