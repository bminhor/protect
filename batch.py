import re
import os
import sys
import json
import argparse
import subprocess
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qs
from dotenv import load_dotenv
from googleapiclient.discovery import build

load_dotenv()

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
TAGS_ENV = os.getenv("TAGS", "")
TAGS = [t.strip() for t in TAGS_ENV.split(",")] if TAGS_ENV.strip() else []
TARGET_SCRIPT = "main.py"

def parse_yymmdd(date_str):
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%y%m%d").replace(tzinfo=timezone.utc)
    except ValueError:
        print("오류: 날짜 형식은 yymmdd 이어야 합니다.")
        sys.exit(1)

def process_target(target, args, youtube, limit_date):
    print(f"[{target}]")
    
    target_clean = target.strip()
    playlist_id = None
    handle = None

    if "youtube.com" in target_clean or "youtu.be" in target_clean:
        parsed_url = urlparse(target_clean)
        query_params = parse_qs(parsed_url.query)
        if "list" in query_params:
            playlist_id = query_params["list"][0]
        else:
            print(f"오류: '{target_clean}' 은 올바른 재생목록 링크가 아닙니다.")
            return
    elif target_clean.startswith('@'):
        handle = target_clean
    elif target_clean.startswith(("PL", "UU", "UUSH", "UULF", "FL", "RD")) and len(target_clean) > 20:
        playlist_id = target_clean
    else:
        handle = f"@{target_clean}"

    uploads_playlist_id = None

    if handle:
        try:
            channel_res = youtube.channels().list(
                part="id",
                forHandle=handle
            ).execute()
            
            if not channel_res.get("items"):
                print(f"'{handle}' 핸들을 찾을 수 없습니다.")
                return
                
            channel_id = channel_res["items"][0]["id"]
            
            if args.s:
                uploads_playlist_id = channel_id.replace("UC", "UUSH", 1)
            elif args.l:
                uploads_playlist_id = channel_id.replace("UC", "UULF", 1)
            else:
                uploads_playlist_id = channel_id.replace("UC", "UU", 1)
                
        except Exception as e:
            print(f"채널 조회 API 오류: {e}")
            return
    else:
        uploads_playlist_id = playlist_id
        if args.s or args.l:
            print("알림: 재생목록 직접 지정 시 -s, -l 옵션은 적용되지 않습니다.")

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
                v_req = youtube.videos().list(
                    part="snippet",
                    id=",".join(chunk)
                )
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

    print(f"총 {len(filtered_videos)}개의 유효한 영상을 찾았습니다.")

    if not filtered_videos:
        print("실행할 영상이 없어 건너뜁니다.")
        return

    videos_json_str = json.dumps(filtered_videos)
    
    target_name = target_clean
    if "list=" in target_name:
        match = re.search(r'list=([a-zA-Z0-9_-]+)', target_name)
        if match: 
            target_name = match.group(1)
    else:
        target_name = target_name.split('/')[-1]

    safe_target_name = re.sub(r'[\\/*?:"<>|]', "", target_name)

    command = ["python", TARGET_SCRIPT]
    if args.d:
        command.extend(["-d", args.d])
    if args.single:
        command.append("-S")
    
    command.extend(["-o", safe_target_name]) 
    command.append(videos_json_str)
    
    try:
        subprocess.run(command, check=True)
    except KeyboardInterrupt:
        print("사용자에 의해 실행이 중단되었습니다.")
        raise
    except subprocess.CalledProcessError as e:
        print(f"{TARGET_SCRIPT} 실행 중 오류가 발생했습니다. (Exit code: {e.returncode})")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("targets", nargs='+')
    
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-s", action="store_true")
    group.add_argument("-l", action="store_true")
    
    parser.add_argument("-D", metavar="yymmdd")
    parser.add_argument("-d", metavar="yymmdd")
    parser.add_argument("-S", "--single", action="store_true")
    
    args = parser.parse_args()

    if not YOUTUBE_API_KEY:
        print("오류: YOUTUBE_API_KEY를 확인해주세요.")
        return

    limit_date = parse_yymmdd(args.D)
    youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)

    for target in args.targets:
        try:
            process_target(target, args, youtube, limit_date)
        except KeyboardInterrupt:
            print("전체 작업이 취소되었습니다.")
            break

    print("모든 처리가 완료되었습니다.")

if __name__ == "__main__":
    main()