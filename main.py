import os
import re
import json
import argparse
import time
import random
import threading
import pandas as pd
from datetime import datetime, timezone
from dotenv import load_dotenv
from googleapiclient.discovery import build
from google import genai
from google.genai import types
from pydantic import BaseModel
from concurrent.futures import ThreadPoolExecutor, as_completed

load_dotenv()

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
MODEL = os.getenv("MODEL")
PROMPT = os.getenv("PROMPT")
MODEL_RPM_DICT = {
    "gemini-3.1-flash-lite-preview": 15,
    "gemini-3-flash-preview": 5
}
RPM_LIMIT = MODEL_RPM_DICT.get(MODEL, 5)

request_times = []
rate_limit_lock = threading.Lock()

def wait_for_rate_limit():
    with rate_limit_lock:
        if len(request_times) >= RPM_LIMIT:
            oldest_time = request_times[0]
            elapsed = time.time() - oldest_time
            
            if elapsed < 60:
                sleep_time = 60 - elapsed + 1
                print(f"⏳ 분당 요청 한도(RPM={RPM_LIMIT}) 초과. {sleep_time:.2f}초 대기 중...")
                time.sleep(sleep_time)

        request_times.append(time.time())
        if len(request_times) > RPM_LIMIT:
            request_times.pop(0)

class CommentAnalysis(BaseModel):
    comment_id: str
    is_sexual_harassment: bool

class BatchAnalysisResponse(BaseModel):
    results: list[CommentAnalysis]

def extract_video_id(url_or_id):
    if len(url_or_id) == 11 and re.match(r'^[0-9A-Za-z_-]{11}$', url_or_id):
        return url_or_id
    
    match = re.search(r'(?:v=|\/|youtu\.be\/|embed\/)([0-9A-Za-z_-]{11})', url_or_id)
    if match:
        return match.group(1)
    
    return None

def get_all_youtube_comments(youtube_client, video_id, since_date=None):
    print(f"https://www.youtube.com/watch?v={video_id}")
    all_comments = []
    next_page_token = None

    while True:
        try:
            request = youtube_client.commentThreads().list(
                part="snippet", videoId=video_id, maxResults=100,
                pageToken=next_page_token, textFormat="plainText"
            )
            response = request.execute()
        except Exception as e:
            error_msg = str(e).lower()
            if "disabledcomments" in error_msg or "comments are disabled" in error_msg:
                print(f"⚠️ 건너뜀: 댓글이 비활성화된 동영상입니다.")
            else:
                print(f"❌ YouTube API 오류: {e}")
            break

        for item in response.get("items", []):
            top_snippet = item["snippet"]["topLevelComment"]["snippet"]
            top_id = item["snippet"]["topLevelComment"]["id"]
            top_published_at = top_snippet["publishedAt"]
            top_dt = datetime.strptime(top_published_at, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            
            if not since_date or top_dt >= since_date:
                all_comments.append({
                    "id": top_id,
                    "author": top_snippet["authorDisplayName"],
                    "authorChannelId": top_snippet.get("authorChannelId", {}).get("value", "알 수 없음"),
                    "text": top_snippet["textDisplay"],
                    "publishedAt": top_published_at,
                    "link": f"https://www.youtube.com/watch?v={video_id}&lc={top_id}"
                })

            if item["snippet"]["totalReplyCount"] > 0:
                try:
                    reply_request = youtube_client.comments().list(
                        part="snippet", parentId=top_id, maxResults=100, textFormat="plainText"
                    )
                    reply_response = reply_request.execute()
                    for reply_item in reply_response.get("items", []):
                        r_snippet = reply_item["snippet"]
                        r_id = reply_item["id"]
                        r_published_at = r_snippet["publishedAt"]
                        
                        reply_dt = datetime.strptime(r_published_at, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
                        
                        if not since_date or reply_dt >= since_date:
                            all_comments.append({
                                "id": r_id,
                                "author": r_snippet["authorDisplayName"],
                                "authorChannelId": r_snippet.get("authorChannelId", {}).get("value", "알 수 없음"),
                                "text": r_snippet["textDisplay"],
                                "publishedAt": r_published_at,
                                "link": f"https://www.youtube.com/watch?v={video_id}&lc={r_id}"
                            })
                except Exception as e:
                    pass

        next_page_token = response.get("nextPageToken")
        if not next_page_token:
            break

    print(f"총 {len(all_comments)}개의 댓글 중,")
    return all_comments

def analyze_comments_batch(gemini_client, comments_batch):
    payload_for_gemini = [{"id": c["id"], "text": c["text"]} for c in comments_batch]
    prompt = f"{PROMPT}\n\n댓글 데이터:\n{json.dumps(payload_for_gemini, ensure_ascii=False)}"
    
    retries = 0
    while True:
        wait_for_rate_limit()
        
        try:
            response = gemini_client.models.generate_content(
                model=MODEL,
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    response_schema=BatchAnalysisResponse,
                    temperature=0.0 
                )
            )
            if not response.text:
                print("⚠️ 빈 응답 수신 (안전 필터링 차단 가능성)")
                return []
                
            return json.loads(response.text).get("results", [])
            
        except json.JSONDecodeError:
            print("⚠️ JSON 파싱 실패. 해당 배치를 건너뜁니다.")
            return []
            
        except Exception as e:
            error_msg = str(e)
            
            if "RequestsPerDay" in error_msg.lower():
                print(f"🚨 일일 요청 한도(RPD) 초과. 내일 다시 시도하거나, 다른 GEMINI_API_KEY를 사용하세요.")
                os._exit(1)
            else:
                sleep_time = (2 ** retries) + random.uniform(1.0, 3.0)
                print(f"⚠️ API 호출 오류 발생. 서버 과부하 가능성. {sleep_time:.2f}초 대기 후 재시도합니다... (재시도: {retries + 1})")
                time.sleep(sleep_time)
                retries += 1

def process_harassment_results(results, batch, found_comments_list):
    for result in results:
        if result.get("is_sexual_harassment"):
            bad_comment = next((c for c in batch if c["id"] == result["comment_id"]), None)
            if bad_comment:
                found_comments_list.append({
                    "작성자": bad_comment["author"],
                    "채널 ID": bad_comment["authorChannelId"],
                    "댓글 내용": bad_comment["text"],
                    "댓글 게시 시간": bad_comment["publishedAt"],
                    "댓글 링크": bad_comment["link"]
                })

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("videos", nargs='+')
    parser.add_argument("-d", "--date")
    parser.add_argument("-S", "--single", action="store_true")
    args = parser.parse_args()

    if not YOUTUBE_API_KEY or not GEMINI_API_KEY:
        print("오류: .env 파일에 API 키를 확인해주세요.")
        return

    since_date = None
    if args.date:
        try:
            since_date = datetime.strptime(args.date, "%y%m%d").replace(tzinfo=timezone.utc)
            print(f"[{since_date.strftime('%Y-%m-%d')}] 이후에 작성된 댓글만 수집합니다.\n")
        except ValueError:
            print("오류: 날짜 형식은 yymmdd 이어야 합니다. (예: 240416)")
            return

    video_list = []
    for v in args.videos:
        if v.startswith('[') and v.endswith(']'):
            try:
                parsed = json.loads(v)
                video_list.extend(parsed)
            except json.JSONDecodeError:
                video_list.append(v)
        else:
            video_list.append(v)

    youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)
    gemini_client = genai.Client(api_key=GEMINI_API_KEY)

    for video_input in video_list:
        video_id = extract_video_id(video_input)
        if not video_id:
            print(f"건너뜀: 유효한 유튜브 비디오 링크나 ID가 아닙니다. ({video_input})")
            continue

        all_comments = get_all_youtube_comments(youtube, video_id, since_date)
        if not all_comments:
            continue

        BATCH_SIZE = 50 
        batches = [all_comments[i:i + BATCH_SIZE] for i in range(0, len(all_comments), BATCH_SIZE)]
        
        found_harassment_comments = []

        if args.single:
            for batch in batches:
                results = analyze_comments_batch(gemini_client, batch)
                process_harassment_results(results, batch, found_harassment_comments)
        else:
            with ThreadPoolExecutor(max_workers=5) as executor:
                future_to_batch = {executor.submit(analyze_comments_batch, gemini_client, batch): batch for batch in batches}
                
                for future in as_completed(future_to_batch):
                    batch = future_to_batch[future]
                    results = future.result()
                    process_harassment_results(results, batch, found_harassment_comments)

        total_found = len(found_harassment_comments)
        print(f"{total_found}개의 부적절한 댓글 식별.")
        
        if total_found > 0:
            os.makedirs('output', exist_ok=True)
            filename = f"output/{video_id}.csv"
            df = pd.DataFrame(found_harassment_comments)
            try:
                df.to_csv(filename, index=False, header=False, encoding='utf-8-sig')
                print(f"CSV 파일 저장 완료: {filename}")
            except PermissionError:
                fallback_filename = f"output/{video_id}_alt_{int(time.time())}.csv"
                df.to_csv(fallback_filename, index=False, header=False, encoding='utf-8-sig')
                print(f"⚠️ CSV 파일 저장 완료: {fallback_filename}")
            except Exception as e:
                print(f"❌ CSV 저장 중 알 수 없는 오류 발생: {e}")

        print()

if __name__ == "__main__":
    main()