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
COMPLETED_LOG_FILE = "log.txt"

def load_completed_videos():
    if os.path.exists(COMPLETED_LOG_FILE):
        with open(COMPLETED_LOG_FILE, "r", encoding="utf-8") as f:
            return set(line.strip() for line in f if line.strip())
    return set()

def mark_video_completed(video_id):
    with open(COMPLETED_LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{video_id}\n")

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
                    "video_id": video_id,
                    "authorChannelId": top_snippet.get("authorChannelId", {}).get("value", "알 수 없음"),
                    "author": top_snippet["authorDisplayName"],
                    "text": top_snippet["textDisplay"],
                    "link": f"https://www.youtube.com/watch?v={video_id}&lc={top_id}",
                    "publishedAt": top_published_at,
                    "id": top_id
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
                                "video_id": video_id,
                                "authorChannelId": r_snippet.get("authorChannelId", {}).get("value", "알 수 없음"),
                                "author": r_snippet["authorDisplayName"],
                                "text": r_snippet["textDisplay"],
                                "link": f"https://www.youtube.com/watch?v={video_id}&lc={r_id}",
                                "publishedAt": r_published_at,
                                "id": r_id
                            })
                except Exception as e:
                    pass

        next_page_token = response.get("nextPageToken")
        if not next_page_token:
            break

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
                print(f"⚠️ API 오류 발생. {sleep_time:.2f}초 대기 후 재시도... (재시도: {retries + 1})")
                time.sleep(sleep_time)
                retries += 1

def process_harassment_results(results, batch, found_comments_list):
    for result in results:
        if result.get("is_sexual_harassment"):
            bad_comment = next((c for c in batch if c["id"] == result["comment_id"]), None)
            if bad_comment:
                found_comments_list.append({
                    "영상 ID": bad_comment["video_id"],
                    "작성자 ID": bad_comment["authorChannelId"],
                    "작성자 핸들": bad_comment["author"],
                    "댓글 내용": bad_comment["text"],
                    "댓글 링크": bad_comment["link"],
                    "댓글 게시 시간": bad_comment["publishedAt"]
                })

def main():
    start_time = time.time()
    processed_video_count = 0
    grand_total_found = 0
    
    all_comments_pool = []
    pending_videos = []
    found_harassment_comments = []
    is_interrupted = False

    try:
        parser = argparse.ArgumentParser()
        parser.add_argument("videos", nargs='+')
        parser.add_argument("-d", "--date")
        parser.add_argument("-S", "--single", action="store_true")
        parser.add_argument("-o", "--output-name")
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
        video_tags_map = {}

        for v in args.videos:
            if v.startswith('[') and v.endswith(']'):
                try:
                    parsed = json.loads(v)
                    for item in parsed:
                        if isinstance(item, dict) and "id" in item:
                            video_list.append(item["id"])
                            video_tags_map[item["id"]] = item.get("tags", "")
                        else:
                            video_list.append(item)
                            video_tags_map[item] = ""
                except json.JSONDecodeError:
                    video_list.append(v)
                    video_tags_map[v] = ""
            else:
                video_list.append(v)
                video_tags_map[v] = ""

        youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)
        gemini_client = genai.Client(api_key=GEMINI_API_KEY)

        completed_videos = load_completed_videos()

        for video_input in video_list:
            video_id = extract_video_id(video_input)
            if not video_id:
                print(f"건너뜀: 유효한 유튜브 비디오 링크나 ID가 아닙니다. ({video_input})")
                continue

            if video_id in completed_videos:
                print(f"건너뜀: 이미 분석이 완료된 영상입니다. ({video_id})")
                continue
            
            processed_video_count += 1
            comments = get_all_youtube_comments(youtube, video_id, since_date)
            
            if not comments:
                mark_video_completed(video_id) 
                continue

            all_comments_pool.extend(comments)
            pending_videos.append(video_id)

        if not all_comments_pool:
            print("\n분석할 새로운 댓글이 없습니다. 작업을 종료합니다.")
            return

        BATCH_SIZE = 50 
        batches = [all_comments_pool[i:i + BATCH_SIZE] for i in range(0, len(all_comments_pool), BATCH_SIZE)]
        total_batches = len(batches)
        
        print(f"📦 총 {len(pending_videos)}개의 영상에서 {len(all_comments_pool)}개의 댓글을 수집했습니다.")
        print(f"🚀 {BATCH_SIZE}개 단위로 묶어 총 {total_batches}번의 API 요청을 시작합니다...")

        processed_batches = 0

        if args.single:
            for batch in batches:
                results = analyze_comments_batch(gemini_client, batch)
                process_harassment_results(results, batch, found_harassment_comments)
                processed_batches += 1
                print(f"진행 상황: {processed_batches}/{total_batches} 배치 분석 완료", end='\r')
        else:
            with ThreadPoolExecutor(max_workers=5) as executor:
                future_to_batch = {executor.submit(analyze_comments_batch, gemini_client, batch): batch for batch in batches}
                for future in as_completed(future_to_batch):
                    batch = future_to_batch[future]
                    results = future.result()
                    process_harassment_results(results, batch, found_harassment_comments)
                    processed_batches += 1
                    print(f"진행 상황: {processed_batches}/{total_batches} 배치 분석 완료", end='\r')
                    
    except KeyboardInterrupt:
        is_interrupted = True
        print("\n\n" + "🚨" * 25)
        print("사용자에 의해 실행이 강제 중단되었습니다 (Ctrl+C).")
        print("🚨 지금까지 찾은 부적절한 댓글까지만 최대한 저장합니다.")
        print("🚨" * 25 + "\n")
        
    except Exception as e:
        is_interrupted = True
        print(f"\n\n❌ 예상치 못한 오류가 발생했습니다: {e}")

    finally:
        if args.output_name:
            grand_total_found = len(found_harassment_comments)
            if grand_total_found > 0:
                os.makedirs('output', exist_ok=True)
                filename = f"output/{args.output_name}.csv"
                
                formatted_comments = []
                for c in found_harassment_comments:
                    vid = c["영상 ID"]
                    formatted_comments.append({
                        "매칭 태그": video_tags_map.get(vid, ""),
                        "영상 ID": vid,
                        "작성자 ID": c["작성자 ID"],
                        "작성자 핸들": c["작성자 핸들"],
                        "댓글 내용": c["댓글 내용"],
                        "댓글 링크": c["댓글 링크"],
                        "댓글 게시 시간": c["댓글 게시 시간"]
                    })
                    
                df = pd.DataFrame(formatted_comments)
                file_exists = os.path.isfile(filename)
                
                try:
                    df.to_csv(filename, mode='a', index=False, header=not file_exists, encoding='utf-8-sig')
                    print(f"📁 통합 CSV 저장 완료: {filename} (적발 {grand_total_found}개)")
                except PermissionError:
                    fallback_filename = f"output/{args.output_name}_alt_{int(time.time())}.csv"
                    df.to_csv(fallback_filename, mode='a', index=False, header=not file_exists, encoding='utf-8-sig')
                    print(f"⚠️ 권한 오류로 대체 파일명 저장 완료: {fallback_filename}")
                except Exception as e:
                    print(f"❌ 통합 CSV 저장 오류: {e}")
                    
            if not is_interrupted:
                for vid in pending_videos:
                    mark_video_completed(vid)

        else:
            results_by_video = {vid: [] for vid in pending_videos}
            
            for comment in found_harassment_comments:
                vid = comment.get("영상 ID")
                if vid in results_by_video:
                    clean_comment = {
                        "매칭 태그": video_tags_map.get(vid, ""),
                        "영상 ID": vid,
                        "작성자 ID": comment["작성자 ID"],
                        "작성자 핸들": comment["작성자 핸들"],
                        "댓글 내용": comment["댓글 내용"],
                        "댓글 링크": comment["댓글 링크"],
                        "댓글 게시 시간": comment["댓글 게시 시간"]
                    }
                    results_by_video[vid].append(clean_comment)

            for vid in pending_videos:
                bad_comments = results_by_video.get(vid, [])
                total_found = len(bad_comments)
                grand_total_found += total_found
                
                if total_found > 0:
                    os.makedirs('output', exist_ok=True)
                    filename = f"output/{vid}.csv"
                    df = pd.DataFrame(bad_comments)
                    file_exists = os.path.isfile(filename)
                    
                    try:
                        df.to_csv(filename, mode='a', index=False, header=not file_exists, encoding='utf-8-sig')
                        print(f"[{vid}] {total_found}개 적발 -> {filename} 저장 완료")
                    except PermissionError:
                        fallback_filename = f"output/{vid}_alt_{int(time.time())}.csv"
                        df.to_csv(fallback_filename, mode='a', index=False, header=not file_exists, encoding='utf-8-sig')
                        print(f"[{vid}] {total_found}개 적발 -> ⚠️ {fallback_filename} 저장 완료")
                    except Exception as e:
                        print(f"[{vid}] ❌ CSV 저장 오류: {e}")
                
                if not is_interrupted:
                    mark_video_completed(vid)

        end_time = time.time()
        elapsed_time = end_time - start_time
        hours, rem = divmod(elapsed_time, 3600)
        minutes, seconds = divmod(rem, 60)
        
        print("📊 [작업 종합 리포트]")
        print(f"⏱️ 총 실행 시간: {int(hours)}시간 {int(minutes)}분 {seconds:.2f}초")
        print(f"🎬 분석(시도)한 영상 수: {processed_video_count}개")
        print(f"🚨 총 식별된 부적절한 댓글 수: {grand_total_found}개")

if __name__ == "__main__":
    main()