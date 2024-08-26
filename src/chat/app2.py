from textual import on
from rich.text import Text
from textual.app import App, ComposeResult
from textual.widgets import Input, RichLog, Static
from kafka import KafkaProducer, KafkaConsumer
from json import dumps, loads
from datetime import datetime
import threading

class ChatApp(App):
    def __init__(self):
        super().__init__()
        self.user_name = None  # 사용자 이름을 저장할 속성
        self.producer = KafkaProducer(
            bootstrap_servers=['172.17.0.1:9092'],
            value_serializer=lambda x: dumps(x).encode('utf-8')
        )
        self.consumer_thread = threading.Thread(target=self.consume_messages, daemon=True)
        self.consumer_thread.start()

    ############ UI 구성하는곳 #############
    def compose(self) -> ComposeResult:
        # 사용자 이름 입력을 위한 초기 화면
        yield Static("사용자 이름을 입력하세요:", id="user_name_prompt")
        yield Input(placeholder="이름을 입력하고 Enter 키를 누르세요...", id="user_name_input")

    ################ 메세지 관련 ##################
    @on(Input.Submitted, "#user_name_input")
    def on_user_name_submitted(self, event: Input.Submitted):
        """사용자가 이름을 입력했을 때 호출됩니다."""
        self.user_name = event.value.strip()  # 사용자 이름을 저장
        self.query_one("#user_name_prompt").remove()  # 이름 입력 위젯 제거
        self.query_one("#user_name_input").remove()
        
        # 채팅 입력 화면 설정
        self.compose_chat_screen()

    def compose_chat_screen(self):
        """채팅 화면을 구성합니다."""
        self.mount(RichLog(id="chat_log"))  # 채팅 로그를 표시하는 위젯
        self.mount(Input(placeholder="메시지를 입력하세요...", id="message_input"))  # 메시지 입력 위젯

    @on(Input.Submitted, "#message_input")
    def on_input_submitted(self, event: Input.Submitted):  # producer
        input_widget = self.query_one("#message_input", Input)
        log_widget = self.query_one("#chat_log", RichLog)

        # 입력한 메시지 받음
        message = event.value
        # exit 입력시 종료
        if message.lower() == 'exit':
            self.exit()
        data = {
            'sender': self.user_name,  # 입력받은 사용자 이름을 사용
            'message': message,
            'time': datetime.today().strftime("%H:%M")
        }
        self.producer.send('input', value=data)
        self.producer.flush()

        # 메시지를 로그에 추가
        # 여기에서 producer 출력
        text_prod = Text(f"{data['sender']}: {message} (보낸 시간: {data['time']})",
                style="bold green")  # 입력 들어오는거 꾸미기
        log_widget.write(text_prod)

        # 입력 필드 초기화
        input_widget.value = ""

    def consume_messages(self):  # consumer
        consumer = KafkaConsumer(
            'input',
            bootstrap_servers=["172.17.0.1:9092"],
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id='chat_group',
            value_deserializer=lambda x: loads(x.decode('utf-8'))
        )
        try:
            for msg in consumer:
                data = msg.value
                sender = data['sender']
                message = data['message']
                received_time = data['time']
                if sender != self.user_name:  # 내가 보낸건 보고싶지 않아요
                    self.post_message_to_log(sender, message, received_time)
        except KeyboardInterrupt:
            print("채팅 종료")
        finally:
            consumer.close()

    def post_message_to_log(self, sender, message, received_time):
        log_widget = self.query_one("#chat_log", RichLog)
        # 여기에서 consumer 값 출력
        text_con = Text(f"{sender} : {message} (받은 시간 : {received_time})",
                style="bold blue", justify="right")  # 받는 채팅은 우측으로
        log_widget.write(text_con)

if __name__ == "__main__":
    app = ChatApp()
    app.run()

