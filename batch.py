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
        print("❌ 오류: 날짜 형식은 yymmdd 이어야 합니다. (예: 240416)")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="YouTube 채널 또는 재생목록 영상 일괄 쿼리 및 실행기")
    parser.add_argument("target", help="채널 핸들(예: @KBSKpop), 재생목록 ID(PL...), 또는 재생목록 전체 링크")
    
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-s", action="store_true", help="쇼츠(Shorts) 영상만 쿼리 (채널 핸들 입력 시에만 동작)")
    group.add_argument("-l", action="store_true", help="일반(Long-form) 영상만 쿼리 (채널 핸들 입력 시에만 동작)")
    
    parser.add_argument("-D", metavar="yymmdd", help="해당 날짜 이후 업로드된 영상만 쿼리")
    parser.add_argument("-d", metavar="yymmdd", help="main.py에 전달할 댓글 쿼리 날짜")

    parser.add_argument("-S", "--single", action="store_true", help="main.py를 싱글 스레드 모드로 실행")
    args = parser.parse_args()

    if not YOUTUBE_API_KEY:
        print("❌ 오류: YOUTUBE_API_KEY를 확인해주세요.")
        return

    target = args.target.strip()
    playlist_id = None
    handle = None

    if "youtube.com" in target or "youtu.be" in target:
        parsed_url = urlparse(target)
        query_params = parse_qs(parsed_url.query)
        if "list" in query_params:
            playlist_id = query_params["list"][0]
        else:
            print("❌ 오류: 올바른 재생목록 링크가 아닙니다.")
            return

    elif target.startswith('@'):
        handle = target

    elif target.startswith(("PL", "UU", "UUSH", "UULF", "FL", "RD")) and len(target) > 20:
        playlist_id = target
        
    else:
        handle = f"@{target}"

    limit_date = parse_yymmdd(args.D)
    youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)
    uploads_playlist_id = None

    if handle:
        try:
            channel_res = youtube.channels().list(
                part="id",
                forHandle=handle
            ).execute()
            
            if not channel_res.get("items"):
                print(f"❌ '{handle}' 핸들을 가진 채널을 찾을 수 없습니다.")
                return
                
            channel_id = channel_res["items"][0]["id"]
            
            if args.s:
                uploads_playlist_id = channel_id.replace("UC", "UUSH", 1)
            elif args.l:
                uploads_playlist_id = channel_id.replace("UC", "UULF", 1)
            else:
                uploads_playlist_id = channel_id.replace("UC", "UU", 1)
                
        except Exception as e:
            print(f"❌ 채널 조회 API 오류: {e}")
            return
    else:
        uploads_playlist_id = playlist_id
        if args.s or args.l:
            print("⚠️ 알림: 재생목록을 직접 지정한 경우 -s, -l 옵션은 적용되지 않습니다.")

    video_ids = []
    next_page_token = None
    fetch_done = False

    print(f"🔍 재생목록({uploads_playlist_id})에서 영상을 불러오는 중...")

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
            print(f"❌ PlaylistItems API 오류 (권한이 없거나 삭제된 재생목록일 수 있습니다): {e}")
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
        print("🔍 태그 기반 필터링 진행 중...")
        chunks = [video_ids[i:i+50] for i in range(0, len(video_ids), 50)]
        for chunk in chunks:
            try:
                v_req = youtube.videos().list(
                    part="snippet",
                    id=",".join(chunk)
                )
                v_res = v_req.execute()
            except Exception as e:
                print(f"❌ Videos API 오류: {e}")
                continue

            for v_item in v_res.get("items", []):
                v_id = v_item["id"]
                title = v_item["snippet"].get("title", "")
                tags = v_item["snippet"].get("tags", [])
                
                has_tag = False
                for tag in TAGS:
                    tag_lower = tag.lower()
                    if tag_lower in title.lower() or any(tag_lower in t.lower() for t in tags):
                        has_tag = True
                        break
                if has_tag:
                    filtered_videos.append(v_id)
    else:
        filtered_videos = video_ids

    print(f"✅ 총 {len(filtered_videos)}개의 유효한 영상을 찾았습니다.")

    if not filtered_videos:
        print("실행할 영상이 없습니다. 작업을 종료합니다.")
        return

    videos_json_str = json.dumps(filtered_videos)
    
    target_name = args.target
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

    print("-" * 50)
    print(f"🚀 {TARGET_SCRIPT} 실행 중...")
    print("-" * 50)
    
    try:
        subprocess.run(command, check=True)
    except KeyboardInterrupt:
        print("\n⚠️ 사용자에 의해 실행이 중단되었습니다.")
    except subprocess.CalledProcessError as e:
        print(f"\n❌ {TARGET_SCRIPT} 실행 중 오류가 발생했습니다. (Exit code: {e.returncode})")

if __name__ == "__main__":
    main()