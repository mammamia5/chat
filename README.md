## 채팅 프로그램 및 챗봇 기능 구현

### 배경
사내기술 유출 이슈로 인해 감사팀 메신저 감찰 필요

### 주요 기능
1. 대화 기능
- 채팅방 입장시 ID 설정 기능
- 입장시 환영 및 입장 메시지 출력 기능
- 퇴장시 퇴장 메시지 출력 기능
2. 알림 기능
- Airflow 성공 시 성공 알림 메시지 전송
- 특정 시간(칸반미팅 시간) 알림 메시지 전송
3. 챗봇 기능
- 영화 검색 기능 : @영화검색 <영화이름> 입력 시 검색어가 포함된 영화 정보 출력
- 메시지 검색 기능 : @검색 <검색명> 입력 시 검색어가 포함된 채팅 내용 출력
4. 데이터 저장
- Airflow를 활용하여 채팅 로그를 parquet파일로 저장

### 기술스택
- Apache Kafka
- Apache Spark
- Apache Airflow
- Apache Zeppelin
- Textualize

### 설정 및 실행
**Airflow 환경설정**
```bash
$ cat ~/.zshrc

export AIRFLOW_HOME=~/pj2/airflow
export AIRFLOW__CORE__DAGS_FOLDER=~/pj2/airflow/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False
```

**Kafka chatting program 실행**
```bash
$ source .venv/bin/activate
$ python src/chat/name.py
```

### 구조
**dags**
![image](https://github.com/user-attachments/assets/9e83751e-7750-4ff0-96b2-fdc2a7532e40)
**chat**
![image](https://github.com/user-attachments/assets/a1ff8b06-730a-4c82-b0b4-da94a79a2e31)


### 결과
![image](https://github.com/user-attachments/assets/f60899e7-ac7f-4ad2-8c3c-5ed1a4cf892a)




