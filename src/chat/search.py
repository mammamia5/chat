from textual import on
from rich.text import Text
from textual.app import App, ComposeResult
from textual.widgets import Input, RichLog
from kafka import KafkaProducer, KafkaConsumer
from json import dumps, loads
from datetime import datetime
import threading
from textual.widgets import Header, Footer, Input
from rich.console import Console

class Mammamia(App):
    def __init__(self):
        super().__init__()
        self.producer = KafkaProducer(
            bootstrap_servers=['ec2-43-203-210-250.ap-northeast-2.compute.amazonaws.com:9092'],
            value_serializer=lambda x: dumps(x).encode('utf-8')
        )
        self.consumer_thread = threading.Thread(target=self.consume_messages, daemon=True)
        self.consumer_thread.start()
        
        # 메시지 로그를 저장할 리스트 (초기화)
        self.messages = []
        self.console = Console()

    ############ UI 구성하는곳 #############
    def compose(self) -> ComposeResult:
        # TODO : 시작할때 본인 이름을 치는걸 구현하고 싶어요
        yield Header()
        rich_log = RichLog()
        rich_log.styles.background = '#ffffff'
        yield rich_log
        #yield RichLog()
        yield Input(placeholder="메시지를 입력하세요...") # 채팅창 입력칸
        
    def on_mount(self) -> None:
        self.title = "KAFKA CHATTING PROGRAM"
        self.sub_title = "TEAM mammamia"
        self.screen.styles.background = "white"
        self.screen.styles.border = ("heavy", "lightgrey")
        #self.chatroom.styles.background = "#f9f9f9"
        #self.chatroom.styles.text_color = "black"

        # 채팅방 입장 메시지 자동으로 출력 및 전송
        self.send_entry_message()

    ################ 메세지 관련 ##################
    @on(Input.Submitted) # 채팅 치면 발생하는 event
    def on_input_submitted(self, event: Input.Submitted): # producer
        input_widget = self.query_one(Input)
        log_widget = self.query_one(RichLog)
        
        # 입력한 메시지 받음
        message = event.value
        # exit 입력시 종료
        if message.lower() == 'exit':
            self.send_exit_message()
            self.exit()
            return #return으로 함수를 끝내야 exit이 중복으로 나오지 않음
        
        # 검색 명령어 처리: @검색 키워드
        if message.startswith('@검색'):
            keyword = message.split(' ', 1)[1] if ' ' in message else None
            if keyword:
                self.search_messages(keyword)
            else:
                log_widget.write(Text("검색어를 입력하세요.", style="bold red"))
            input_widget.value = ""  # 입력 필드 초기화
            return
        
        input_widget.value = ""  # 입력 필드 초기화
        
        # 일반 메시지 처리
        data = {
            'sender': '박민주',  # 사용자 이름을 입력하고 시작하는 식으로 고칠까
            'message': message,
            'time': datetime.today().strftime("%Y-%m-%d %H:%M:%S")}
        self.producer.send('mammamia10', value=data)
        self.producer.flush()
        
        # 메시지를 로그에 추가
        # 여기에서 producer 출력
        text_prod = Text(f"{data['sender']}: {message} (보낸 시간: {data['time']})", style="#000000") # 입력 들어오는거 꾸미기
        log_widget.write(text_prod)
        

    def search_messages(self, keyword):
        log_widget = self.query_one(RichLog)
        # 검색 결과 로그에 출력
        log_widget.write(Text(f"'{keyword}'에 대한 검색 결과:", style="bold green"))

        # 검색할 메시지가 저장된 로그를 순회 (간단한 예로 self.messages 리스트 사용)
        search_results = [f"{msg['sender']} : {msg['message']} (보낸 시간 : {msg['time']})"
                      for msg in self.messages if keyword in msg['message']]

        if search_results:
            for result in search_results:
                log_widget.write(Text(result, style="bold yellow"))
        else:
            log_widget.write(Text("검색 결과가 없습니다.", style="bold red"))

    def send_exit_message(self):
        log_widget = self.query_one(RichLog)
        log_widget.write(Text("채팅을 종료합니다.", style="bold red"))



    def send_entry_message(self):
        log_widget = self.query_one(RichLog)
        entry_message = {
        'sender': '박민주',
        'message':'채팅방에 입장하셨습니다.',
        'time': datetime.today().strftime("%Y-%m-%d %H:%M:%S")}
    
    # producer가 입장 메시지를 보냄
        self.producer.send('mammamia10', value=entry_message)
        self.producer.flush()
    
    # 입장 메시지를 로그에 추가
        entry_text = Text(f"{entry_message['sender']}님이 {entry_message['message']} (보낸 시간: {entry_message['time']})", style="#fca311", justify="right")
        log_widget.write(entry_text)

    def send_exit_message(self):
        log_widget = self.query_one(RichLog)
        exit_message = {
        'sender': '박민주',
        'message':'채팅방을 퇴장했습니다.',
        'time': datetime.today().strftime("%Y-%m-%d %H:%M:%S")}

    # producer가 퇴장 메시지를 보냄
        self.producer.send('mammamia10', value=exit_message)
        self.producer.flush()

    # 퇴장 메시지를 로그에 추가
        exit_text = Text(f"{exit_message['sender']}님이 {exit_message['message']} (보낸 시간: {exit_message['time']})", style="#e5e5e5", justify="right")
        log_widget.write(exit_text)
        
    def consume_messages(self): # consumer
        consumer = KafkaConsumer(
            'mammamia10',
            bootstrap_servers=["ec2-43-203-210-250.ap-northeast-2.compute.amazonaws.com:9092"],
            auto_offset_reset="earliest",
            #enable_auto_commit=True,
            #group_id='chat_group',
            value_deserializer=lambda x: loads(x.decode('utf-8'))
        )
        try:
            for msg in consumer:
                data = msg.value
                sender = data['sender']
                message = data['message']
                received_time = data['time']

                self.messages.append(data)

                if sender != '박민주': # 내가 보낸건 보고싶지않아요
                    self.post_message_to_log(sender, message, received_time)
        except KeyboardInterrupt:
            print("채팅 종료")
        finally:
            consumer.close()

    def post_message_to_log(self, sender, message, received_time):
        log_widget = self.query_one(RichLog)
        if "퇴장했습니다" in message:
            text_con = Text(f"{sender}님이 {message} (받은 시간 : {received_time})", style="#e5e5e5", justify="right")
        elif "입장하셨습니다" in message:
            text_con = Text(f"{sender}님이 {message} (받은 시간 : {received_time})", style="#fca311", justify="right")    
        else:
            text_con = Text(f"{sender} : {message} (받은 시간 : {received_time})", style="#14213d", justify="right")
        # 여기에서 consumer 값 출력
        #if message == 'exit':
        #    self.send_exit_message()
        #else:
        #text_con = Text(f"{sender} : {message} (받은 시간 : {received_time})", style="bold white", justify="right") # 받는 채팅은 우측으로
        #log_widget.write(f"{sender} : {message} (받은 시간 : {received_time})")
        log_widget.write(text_con)

if __name__ == "__main__":
    app = Mammamia()
    app.run()
