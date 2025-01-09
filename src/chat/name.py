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
from textual.containers import Container
from pprint import pformat
from chat.movie_search import read_data


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
        self.user_name = None  # 사용자의 이름을 저장할 변수

    ############ UI 구성하는곳 #############
    def compose(self) -> ComposeResult:
        yield Header()
        yield Input(placeholder="이름을 입력하세요...", id="name_input")  # 이름 입력을 위한 Input 위젯
        rich_log = RichLog(id="chat_log")
        rich_log.styles.display = "none"

        rich_log.styles.background = '#fcecd9'
        yield rich_log
        message_input = Input(placeholder="메시지를 입력하세요...", id="message_input", disabled=True)
        message_input.styles.display = "none"  # 초기에는 숨겨진 메시지 입력 필드
        yield message_input
        
    def on_mount(self) -> None:
        self.title = "KAFKA CHATTING PROGRAM"
        self.sub_title = "TEAM mammamia"
        self.screen.styles.background = "#fee6c2"
        self.screen.styles.border = ("heavy", "black")
        #self.chatroom.styles.background = "#f9f9f9"
        #self.chatroom.styles.text_color = "black"

        # 채팅방 입장 메시지 자동으로 출력 및 전송
        #self.send_entry_message()

    ################ 메세지 관련 ##################
    @on(Input.Submitted) # 채팅 치면 발생하는 event
    def on_input_submitted(self, event: Input.Submitted): # producer
        #input_widget = self.query_one(Input)
        input_widget = event.input  # 입력이 발생한 위젯을 가져옵니다.
        log_widget = self.query_one("#chat_log")
        
        if input_widget.id == "name_input":
            # 이름 입력을 처리
            self.user_name = event.value.strip()  # 입력된 이름을 저장
            if self.user_name:
                # 환영 메시지를 출력하고 메시지 입력 필드로 전환
                log_widget.write(Text(f"환영합니다, {self.user_name}님! 이제 채팅을 시작할 수 있습니다.", style="bold green"))
                input_widget.styles.display = "none"  # 이름 입력 필드를 숨김
                log_widget.styles.display = "block"  # 채팅 로그를 표시
                message_input = self.query_one("#message_input")
                message_input.styles.display = "block"  # 메시지 입력 필드를 표시
                message_input.disabled = False  # 메시지 입력 필드를 활성화
                message_input.focus()  # 메시지 입력 필드에 포커스를 줍니다.

                # 이름 입력 후 입장 메시지 전송
                self.send_entry_message()
            else:
                log_widget.write(Text("이름을 입력하세요.", style="bold red"))
            
            input_widget.value = ""
            return

        elif input_widget.id == "message_input":
            # 메시지 입력을 처리
            message = event.value.strip()
            if not message:
                return

            if message.lower() == 'exit':
                self.send_exit_message()
                self.exit()
                return  # return으로 함수를 끝내야 exit이 중복으로 나오지 않음

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
        elif message.startswith('@영화검색'):
            keyword = message.split(' ', 1)[1] if ' ' in message else None
            if keyword:
                df = read_data(keyword)
                data = {
                    'sender': keyword,  # 사용자 이름을 입력하고 시작하는 식으로 고칠까
                    'message': df,
                    'time': datetime.today().strftime("%Y-%m-%d %H:%M:%S")}
                self.producer.send('mammamia10', value=data)
                self.producer.flush()

                # 메시지를 로그에 추가
                # 여기에서 producer 출력
                text_prod = Text(f"{data['sender']}: {message} (보낸 시간: {data['time']})", style="#000000") # 입력 들>어오는거 꾸미기
                log_widget.write(text_prod)
            else:
                log_widget.write(Text("검색어를 입력하세요.", style="bold red"))
            input_widget.value = ""
            return        
        #input_widget.value = ""  # 입력 필드 초기화
        

        # 일반 메시지 처리
        data = {
            'sender': self.user_name,
            'message': message,
            'time': datetime.today().strftime("%Y-%m-%d %H:%M:%S")}
        self.producer.send('mammamia10', value=data)
        self.producer.flush()
        
        # 메시지를 로그에 추가
        # 여기에서 producer 출력
        text_prod = Text(f"{data['sender']}: {message} (보낸 시간: {data['time']})", style="#000000") # 입력 들어오는거 꾸미기
        log_widget.write(text_prod)
        
        input_widget.value = ""  # 입력 필드 초기화

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
        if not self.user_name:  # user_name이 None이거나 비어 있으면 메시지를 전송하지 않음
            return

        log_widget = self.query_one("#chat_log")
        entry_message = {
        'sender': self.user_name,
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
        'sender': self.user_name,
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
            group_id='chat_group',
            value_deserializer=lambda x: loads(x.decode('utf-8'))
        )
        try:
            for msg in consumer:
                data = msg.value
                sender = data['sender']
                message = data['message']
                received_time = data['time']

                self.messages.append(data)
                
                if sender != self.user_name: # 내가 보낸건 보고싶지않아요
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
        elif type(message) == list:
            pmessage = pformat(message)
            text_con = Text(f"{sender} : {pmessage} (받은 시간 : {received_time})", style="bold #784aec")
        else:
            text_con = Text(f"{sender} : {message} (받은 시간 : {received_time})", style="bold #fd01d3", justify="right")
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
