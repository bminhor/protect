import os
import re
import sys
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
TAGS_ENV = os.getenv("TAGS", "")
TAGS = [t.strip() for t in TAGS_ENV.split(",")] if TAGS_ENV.strip() else []
MODEL_RPM_DICT = {
    "gemini-3.1-flash-lite-preview": 15,
    "gemini-3-flash-preview": 5
}
RPM_LIMIT = MODEL_RPM_DICT.get(MODEL, 5)

request_times = []
rate_limit_lock = threading.Lock()

class FatalAPIError(Exception):
    pass

class CommentAnalysis(BaseModel):
    comment_id: str
    is_sexual_harassment: bool

class BatchAnalysisResponse(BaseModel):
    results: list[CommentAnalysis]

def wait_for_rate_limit():
    with rate_limit_lock:
        if len(request_times) >= RPM_LIMIT:
            oldest_time = request_times[0]
            elapsed = time.time() - oldest_time
            if elapsed < 60:
                sleep_time = 60 - elapsed + 1
                print(f"분당 요청 한도 초과. {sleep_time:.2f}초 대기 중...")
                time.sleep(sleep_time)
        request_times.append(time.time())
        if len(request_times) > RPM_LIMIT:
            request_times.pop(0)

def parse_yymmdd(date_str):
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%y%m%d").replace(tzinfo=timezone.utc)
    except ValueError:
        print("오류: 날짜 형식은 yymmdd 이어야 합니다.")
        sys.exit(1)

def get_target_type(target):
    if target.startswith('@'):
        return 'channel'
    elif target.startswith('PL') and len(target) == 34:
        return 'playlist'
    elif len(target) == 11:
        return 'video'
    else:
        return 'unknown'

def fetch_videos_from_target(youtube, target, target_type, args, limit_date):
    uploads_playlist_id = None
    
    if target_type == 'channel':
        try:
            channel_res = youtube.channels().list(part="id", forHandle=target).execute()
            if not channel_res.get("items"):
                print(f"'{target}' 핸들을 찾을 수 없습니다.")
                return []
            channel_id = channel_res["items"][0]["id"]
            if args.s:
                uploads_playlist_id = channel_id.replace("UC", "UUSH", 1)
            elif args.l:
                uploads_playlist_id = channel_id.replace("UC", "UULF", 1)
            else:
                uploads_playlist_id = channel_id.replace("UC", "UU", 1)
        except Exception as e:
            print(f"채널 조회 API 오류: {e}")
            return []
    elif target_type == 'playlist':
        uploads_playlist_id = target
        if args.s or args.l:
            print("알림: 재생목록 지정 시 -s, -l 옵션은 무시됩니다.")
            
    video_ids = []
    next_page_token = None
    fetch_done = False

    while not fetch_done:
        try:
            pl_request = youtube.playlistItems().list(
                part="snippet,contentDetails",
                playlistId=uploads_playlist_id,
                maxResults=50,
                pageToken=next_page_token
            )
            pl_response = pl_request.execute()
        except Exception as e:
            print(f"PlaylistItems API 오류: {e}")
            break

        for item in pl_response.get("items", []):
            pub_date_str = item.get("contentDetails", {}).get("videoPublishedAt") or item["snippet"]["publishedAt"]
            try:
                pub_date = datetime.strptime(pub_date_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            except ValueError:
                continue
            
            if limit_date and pub_date < limit_date:
                fetch_done = True
                break
                
            video_id = item["snippet"]["resourceId"]["videoId"]
            if video_id:
                video_ids.append(video_id)

        next_page_token = pl_response.get("nextPageToken")
        if not next_page_token:
            break

    filtered_videos = []
    
    if TAGS and video_ids:
        chunks = [video_ids[i:i+50] for i in range(0, len(video_ids), 50)]
        for chunk in chunks:
            try:
                v_req = youtube.videos().list(part="snippet", id=",".join(chunk))
                v_res = v_req.execute()
            except Exception as e:
                print(f"Videos API 오류: {e}")
                continue

            for v_item in v_res.get("items", []):
                v_id = v_item["id"]
                title = v_item["snippet"].get("title", "")
                tags = v_item["snippet"].get("tags", [])
                
                matched_tags = []
                for tag in TAGS:
                    tag_lower = tag.lower()
                    if tag_lower in title.lower() or any(tag_lower in t.lower() for t in tags):
                        matched_tags.append(tag)
                        
                if matched_tags:
                    filtered_videos.append({"id": v_id, "tags": ", ".join(matched_tags)})
    else:
        filtered_videos = [{"id": v_id, "tags": ""} for v_id in video_ids]

    return filtered_videos

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
                print("건너뜀: 댓글이 비활성화된 동영상입니다.")
            else:
                print(f"YouTube API 오류: {e}")
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
                except Exception:
                    pass

        next_page_token = response.get("nextPageToken")
        if not next_page_token:
            break

    return all_comments

def analyze_comments_batch(gemini_client, comments_batch, stop_event):
    if stop_event.is_set():
        return []
    payload_for_gemini = [{"id": c["id"], "text": c["text"]} for c in comments_batch]
    prompt_with_data = f"{PROMPT} 댓글 데이터: {json.dumps(payload_for_gemini, ensure_ascii=False)}"
    tries = 1
    while True:
        wait_for_rate_limit()
        try:
            response = gemini_client.models.generate_content(
                model=MODEL,
                contents=prompt_with_data,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    response_schema=BatchAnalysisResponse,
                    temperature=0.0 
                )
            )
            if not response.text:
                print("빈 응답 수신")
                return []
            return json.loads(response.text).get("results", [])
        except json.JSONDecodeError:
            print("JSON 파싱 실패")
            return []
        except Exception as e:
            error_msg = str(e)
            print(error_msg)
            if "RequestsPerDay" in error_msg:
                raise FatalAPIError("일일 요청 한도 초과")
            elif tries == 5:
                raise FatalAPIError("API 오류 지속")
            else:
                sleep_time = (2 ** tries) + random.uniform(2.0, 6.0)
                print(f"API 오류 발생. {sleep_time:.2f}초 대기 후 재시도...")
                time.sleep(sleep_time)
                tries += 1

def process_harassment_results(results, batch, found_comments_list):
    for result in results:
        if result.get("is_sexual_harassment"):
            bad_comment = next((c for c in batch if c["id"] == result["comment_id"]), None)
            if bad_comment:
                found_comments_list.append({
                    "매칭 태그": bad_comment.get("video_tags", ""),
                    "영상 ID": bad_comment["video_id"],
                    "작성자 ID": bad_comment["authorChannelId"],
                    "작성자 핸들": bad_comment["author"],
                    "댓글 내용": bad_comment["text"],
                    "댓글 링크": bad_comment["link"],
                    "댓글 게시 시간": bad_comment["publishedAt"]
                })

def fetch_all_target_data(youtube, target, first_type, args, video_limit_date, comment_limit_date):
    target_videos = []
    video_tags_map = {}

    if first_type == 'video':
        target_videos.append({"id": target, "tags": ""})
    else:
        fetched = fetch_videos_from_target(youtube, target, first_type, args, video_limit_date)
        target_videos.extend(fetched)

    for v in target_videos:
        video_tags_map[v["id"]] = v.get("tags", "")

    fetched_comments = []
    
    for v in target_videos:
        vid = v["id"]
        comments = get_all_youtube_comments(youtube, vid, comment_limit_date)
        for c in comments:
            c["video_tags"] = video_tags_map.get(vid, "")
            c["is_analyzed"] = False
        fetched_comments.extend(comments)
        
    return fetched_comments

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("targets", nargs='+')
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-s", action="store_true")
    group.add_argument("-l", action="store_true")
    parser.add_argument("-D", metavar="yymmdd")
    parser.add_argument("-d", metavar="yymmdd")
    args = parser.parse_args()

    if not YOUTUBE_API_KEY or not GEMINI_API_KEY:
        print("오류: API 키를 확인해주세요.")
        return

    first_type = get_target_type(args.targets[0])
    if first_type == 'unknown':
        print(f"오류: 알 수 없는 입력 형식입니다 -> {args.targets[0]}")
        return

    for t in args.targets[1:]:
        if get_target_type(t) != first_type:
            print("오류: 서로 다른 형식의 타겟을 동시에 입력할 수 없습니다.")
            return

    video_limit_date = parse_yymmdd(args.D)
    comment_limit_date = parse_yymmdd(args.d)
    
    youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)
    gemini_client = genai.Client(api_key=GEMINI_API_KEY)

    os.makedirs('cache', exist_ok=True)
    os.makedirs('output', exist_ok=True)

    total_grand_found = 0
    start_time = time.time()

    for target in args.targets:
        print(f"[{target}] 작업 시작")
        
        safe_target = re.sub(r'[\\/*?:"<>|]', "", target)
        json_filename = f"cache/{safe_target}.json"
        csv_filename = f"output/{safe_target}.csv"
        
        all_comments_pool = []
        
        if os.path.exists(json_filename):
            while True:
                user_input = input(f"{json_filename} 파일이 발견되었습니다. 새로 수집하지 않고 이 파일의 데이터를 불러오시겠습니까? [Y/n]: ")
                if user_input.lower() in ['y', '']:
                    with open(json_filename, 'r', encoding='utf-8') as f:
                        all_comments_pool = json.load(f)
                    break
                elif user_input.lower() == 'n':
                    all_comments_pool = fetch_all_target_data(youtube, target, first_type, args, video_limit_date, comment_limit_date)
                    with open(json_filename, 'w', encoding='utf-8') as f:
                        json.dump(all_comments_pool, f, ensure_ascii=False, indent=2)
                    break
        else:
            all_comments_pool = fetch_all_target_data(youtube, target, first_type, args, video_limit_date, comment_limit_date)
            with open(json_filename, 'w', encoding='utf-8') as f:
                json.dump(all_comments_pool, f, ensure_ascii=False, indent=2)

        has_analyzed_data = any(c.get("is_analyzed", False) for c in all_comments_pool)
        pending_comments_pool = []
        
        if has_analyzed_data:
            while True:
                use_log_input = input("기존 분석을 이어서 진행하시겠습니까? (전체 재분석을 원하시면 n 입력) [Y/n]: ")
                if use_log_input.lower() in ['y', '']:
                    pending_comments_pool = [c for c in all_comments_pool if not c.get("is_analyzed", False)]
                    break
                elif use_log_input.lower() == 'n':
                    for c in all_comments_pool:
                        c["is_analyzed"] = False
                    pending_comments_pool = all_comments_pool
                    break
        else:
            pending_comments_pool = all_comments_pool

        if not pending_comments_pool:
            print("분석할 새로운 댓글이 없습니다.")
            try:
                with open(json_filename, 'w', encoding='utf-8') as f:
                    json.dump(all_comments_pool, f, ensure_ascii=False, indent=2)
            except Exception as e:
                print(f"JSON 저장 오류: {e}")
            continue

        BATCH_SIZE = 50 
        batches = [pending_comments_pool[i:i + BATCH_SIZE] for i in range(0, len(pending_comments_pool), BATCH_SIZE)]
        total_batches = len(batches)
        unique_videos = set(c["video_id"] for c in pending_comments_pool)

        print(f"총 {len(unique_videos)}개의 영상에서 {len(pending_comments_pool)}개의 댓글을 수집했습니다.")
        print(f"{BATCH_SIZE}개 단위로 묶어 총 {total_batches}번의 API 요청을 시작합니다.")

        found_harassment_comments = []
        processed_batches = 0
        stop_event = threading.Event()
        fatal_error_occurred = False
        
        try:
            with ThreadPoolExecutor(max_workers=5) as executor:
                future_to_batch = {executor.submit(analyze_comments_batch, gemini_client, batch, stop_event): batch for batch in batches}
                for future in as_completed(future_to_batch):
                    batch = future_to_batch[future]
                    try:
                        results = future.result()
                        process_harassment_results(results, batch, found_harassment_comments)
                        
                        if not stop_event.is_set():
                            for c in batch:
                                c["is_analyzed"] = True
                                
                        processed_batches += 1
                        print(f"진행 상황: {processed_batches}/{total_batches} 배치 분석 완료")
                    except FatalAPIError as e:
                        print(f"[작업 중단] {e}")
                        print("현재까지 분석된 데이터를 저장하고 안전하게 종료합니다.")
                        stop_event.set()
                        fatal_error_occurred = True
                        break
        except Exception as e:
            print(f"예상치 못한 오류가 발생했습니다: {e}")

        finally:
            total_found = len(found_harassment_comments)
            total_grand_found += total_found

            if found_harassment_comments:
                df = pd.DataFrame(found_harassment_comments)
                file_exists = os.path.isfile(csv_filename)
                try:
                    df.to_csv(csv_filename, mode='a', index=False, header=not file_exists, encoding='utf-8-sig')
                    print(f"{csv_filename} 저장 완료")
                except Exception as e:
                    print(f"CSV 저장 오류: {e}")

            try:
                with open(json_filename, 'w', encoding='utf-8') as f:
                    json.dump(all_comments_pool, f, ensure_ascii=False, indent=2)
                print(f"{json_filename} 업데이트 완료.")
            except Exception as e:
                print(f"JSON 저장 오류: {e}")

            if fatal_error_occurred:
                print("작업이 중간에 중단되었습니다. 다음 실행 시 이어서 작업할 수 있습니다.")
                sys.exit(1)

    end_time = time.time()
    elapsed_time = end_time - start_time
    hours, rem = divmod(elapsed_time, 3600)
    minutes, seconds = divmod(rem, 60)
    
    print(f"총 실행 시간: {int(hours)}시간 {int(minutes)}분 {seconds:.2f}초")
    print(f"총 식별된 부적절한 댓글 수: {total_grand_found}개")

if __name__ == "__main__":
    main()