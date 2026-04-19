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

### 1. 영상 분석

```bash
# 영상 분석 (영상 ID, 띄어쓰기로 구분)
python main.py dQw4w9WgXcQ abcdefg1234

# 특정 날짜(YYMMDD) 이후 작성된 댓글 분석
python main.py dQw4w9WgXcQ -d 260416
```

### 2. 재생목록 또는 채널의 모든 영상 분석

```bash
# 재생목록의 모든 영상 분석 (재생목록 ID, 띄어쓰기로 구분)
python main.py PLxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

# 채널의 모든 영상 분석 (채널 핸들, 띄어쓰기로 구분)
python main.py @KBSKpop @MBCkpop

# 채널의 모든 쇼츠 영상 분석
python main.py @KBSKpop -s

# 채널의 모든 롱폼 영상 분석
python main.py @KBSKpop -l

# 특정 날짜(YYMMDD) 이후 업로드 된 영상 분석
python main.py @KBSKpop -D 260101

# 특정 날짜(YYMMDD) 이후 작성된 댓글 분석
python main.py @KBSKpop -d 260416

# 복합 사용
python main.py @KBSKpop @MBCkpop -D 260101 -d 260416 -s
```

## Output

| 매칭 태그 | 영상 ID | 작성자 ID | 작성자 핸들 | 댓글 내용 | 댓글 링크 | 댓글 게시 시간 |
| --- | --- | --- | --- | --- | --- | --- |

위와 같은 형식으로 `/output/` 경로에 저장됩니다.