import pandas as pd
from collections import Counter
import re

# CSV 파일 경로
file_path = '/home/manggee/data/team_chat.csv'

# CSV 파일 읽기
def read_csv(file_path):
    df = pd.read_csv(file_path, header=None)
    return df

# 텍스트에서 단어 추출
def extract_words(text):
    # 정규 표현식을 사용하여 단어를 추출합니다.
    words = re.findall(r'\b\w+\b', text.lower())
    return words

# 단어 빈도 계산
def count_words(words):
    return Counter(words)

# 메인 함수
def main():
    df = read_csv(file_path)
    all_text = ' '.join(df.iloc[:, 1].astype(str).fillna(''))
    
    # 단어 추출 및 빈도 계산
    words = extract_words(all_text)
    word_count = count_words(words)

    # 가장 많이 나온 단어 10개 출력
    most_common_words = word_count.most_common(10)
    print("가장 많이 나온 단어:")
    for word, count in most_common_words:
        print(f'{word}: {count}')

if __name__ == '__main__':
    main()
