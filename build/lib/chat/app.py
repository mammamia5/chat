from textual import on
from rich.text import Text
from textual.app import App, ComposeResult
from textual.widgets import Input, RichLog
from kafka import KafkaProducer, KafkaConsumer
from json import dumps, loads
from datetime import datetime
import threading
import pandas as pd

class ChatApp(App):
    def __init__(self):
        super().__init__()
        self.producer = KafkaProducer(
            bootstrap_servers=['ec2-43-203-210-250.ap-northeast-2.compute.amazonaws.com:9092'],
            value_serializer=lambda x: dumps(x).encode('utf-8')
        )
        self.consumer_thread = threading.Thread(target=self.consume_messages, daemon=True)
        self.consumer_thread.start()
        self.result = [] # 여기 추가
    ############ UI 구성하는곳 #############
    def compose(self) -> ComposeResult:
        # TODO : 시작할때 본인 이름을 치는걸 구현하고 싶어요

        yield RichLog()
        yield Input(placeholder="메시지를 입력하세요...") # 채팅창 입력칸
    
    ################ 메세지 관련 ##################
    @on(Input.Submitted) # 채팅 치면 발생하는 event
    def on_input_submitted(self, event: Input.Submitted): # producer
        input_widget = self.query_one(Input)
        log_widget = self.query_one(RichLog)
        
        # 입력한 메시지 받음
        message = event.value
        # exit 입력시 종료
        if message.lower() == 'exit':
            # 여기 추가 두 줄
            df = pd.DataFrame(self.result)
            df.to_csv('~/data/team_chat.csv', index = False)
            self.exit()
        data = {
            'sender': '김원준',  # 사용자 이름을 입력하고 시작하는 식으로 고칠까
            'message': message,
            'time': datetime.today().strftime("%H:%M")
        }
        self.producer.send('mammamia3', value=data)
        self.producer.flush()
        
        # 메시지를 로그에 추가
        # 여기에서 producer 출력
#        text_prod = f"{data['sender']}: {message} (보낸 시간: {data['time']})"
        text_prod = Text(f"{data['sender']}: {message} (보낸 시간: {data['time']})",
                style="bold green") # 입력 들어오는거 꾸미기
        #log_widget.write(f"{data['sender']}: {message} (보낸 시간: {data['time']})")
        log_widget.write(text_prod)
        
        # 입력 필드 초기화
        input_widget.value = ""

    def consume_messages(self): # consumer
        consumer = KafkaConsumer(
            'mammamia3',
            bootstrap_servers=["ec2-43-203-210-250.ap-northeast-2.compute.amazonaws.com:9092"],
            auto_offset_reset="earliest",
#            enable_auto_commit=True,
#            group_id='chat_group',
            value_deserializer=lambda x: loads(x.decode('utf-8'))
        )
        try:
            for msg in consumer:
                data = msg.value
                # 여기 추가
                self.result.append(data)
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
        text_con = Text(f"{sender} : {message} (받은 시간 : {received_time})",
                style="bold blue", justify="right")
#        text_con = f"{sender} : {message} (받은 시간 : {received_time})"
        #log_widget.write(f"{sender} : {message} (받은 시간 : {received_time})")
        if len(message) >= 3 and message[:3] == '@검색':
            pass
            #log_widget.write()
        log_widget.write(text_con)

if __name__ == "__main__":
    app = ChatApp()
    app.run()

