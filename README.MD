## Installation
```bash
git clone https://github.com/bminhor/protect.git
cd protect
python -m venv .venv
.venv\Scripts\activate.bat
python -m pip install -U pip
pip install -r requirements.txt
```

## Configuration

`.env.example` 파일을 복사하여 `.env` 파일을 생성한 후, 환경 변수를 알맞게 수정하세요.


## Usage

### 1. 단일 또는 다중 영상 분석 (main.py)

```bash
# 단일 영상 분석 (영상 ID 또는 영상 URL)
python main.py dQw4w9WgXcQ

# 다중 영상 분석
python main.py dQw4w9WgXcQ abcdefg1234

# 특정 날짜(YYMMDD) 이후 작성된 댓글만 분석
python main.py dQw4w9WgXcQ -d 260416

# 싱글 스레드 모드로 실행 (503 에러 방지)
python main.py dQw4w9WgXcQ -S
```

### 2. 채널 또는 재생목록 분석 (batch.py)

```bash
# 채널의 전체 영상 분석 (powershell에서는 @없이)
python batch.py @KBSKpop

# 채널의 쇼츠 영상만 분석
python batch.py @KBSKpop -s

# 채널의 롱폼 영상만 분석
python batch.py @KBSKpop -l

# 특정 날짜(YYMMDD) 이후 업로드 된 영상만 분석
python batch.py @KBSKpop -D 260101

# 특정 날짜(YYMMDD) 이후 작성된 댓글만 분석
python batch.py @KBSKpop -d 260416

# 싱글 스레드 모드로 실행 (503 에러 방지)
python batch.py @KBSKpop -S

# 복합 사용
python batch.py @KBSKpop -D 260101 -d 260416 -s -S

# 재생목록의 전체 영상 분석 (재생목록 ID 또는 재생목록 URL)
python batch.py PLxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

# 다중 채널 및 다중 재생목록의 전체 영상 분석
python batch.py @KBSKpop @MBCkpop PLxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
```

## Output

| 매칭 태그 | 영상 ID | 작성자 ID | 작성자 핸들 | 댓글 내용 | 댓글 링크 | 댓글 게시 시간 |
| --- | --- | --- | --- | --- | --- | --- |

위와 같은 형식으로 `/output/` 경로에 저장됩니다.