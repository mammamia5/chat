from textual import on
from textual.app import App, ComposeResult
from textual.widgets import Input, RichLog
from kafka import KafkaProducer, KafkaConsumer
from json import dumps, loads
from datetime import datetime
import threading

class ChatApp(App):
    def __init__(self):
        super().__init__()
        self.producer = KafkaProducer(
            bootstrap_servers=['172.17.0.1:9092'],
            value_serializer=lambda x: dumps(x).encode('utf-8')
        )
        self.consumer_thread = threading.Thread(target=self.consume_messages, daemon=True)
        self.consumer_thread.start()
    ############ UI 구성하는곳 #############
    def compose(self) -> ComposeResult:
        yield RichLog()
        yield Input(placeholder="메시지를 입력하세요...") # 채팅창
    
    ################ 메세지 관련 ##################
    @on(Input.Submitted) # 채팅 치면 발생하는 event
    def on_input_submitted(self, event: Input.Submitted): # producer
        input_widget = self.query_one(Input)
        log_widget = self.query_one(RichLog)
        
        # Kafka로 메시지 전송
        message = event.value
        data = {
            'sender': '김원준',  # 사용자 이름을 입력하고 시작하는 식으로 고칠까
            'message': message,
            'time': datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        }
        self.producer.send('input', value=data)
        self.producer.flush()
        
        # 메시지를 로그에 추가
        # 여기에서 producer 출력
        log_widget.write(f"{data['sender']}: {message}")
        
        # 입력 필드 초기화
        input_widget.value = ""

    def consume_messages(self): # consumer
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
                if sender != '김원준': # 내가 보낸건 보고싶지않아요
                    self.post_message_to_log(sender, message, received_time)
        except KeyboardInterrupt:
            print("채팅 종료")
        finally:
            consumer.close()

    def post_message_to_log(self, sender, message, received_time):
        log_widget = self.query_one(RichLog)
        # 여기에서 consumer 값 출력
        log_widget.write(f"[{sender}] : {message} (받은 시간 : {received_time})")

if __name__ == "__main__":
    app = ChatApp()
    app.run()

