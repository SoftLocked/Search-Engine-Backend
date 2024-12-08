from nltk.stem import PorterStemmer
import hashlib
from nltk.corpus import stopwords

ps = PorterStemmer()
stop_words = set([ps.stem(i).lower() for i in stopwords.words('english')])

class Token:
    def __init__(self, tok_str: str) -> None:
        self.orig_str = tok_str
        self.tok_str: str = ps.stem(tok_str).lower()
    
    def __eq__(self, value: object) -> bool:
        return self.tok_str == value.tok_str

    def __repr__(self) -> str:
        return self.tok_str
    
    def __hash__(self) -> int:
        return int(hashlib.sha256(self.tok_str.encode()).hexdigest(), 16)
    
    def is_stop(self) -> bool:
        return self.tok_str in stop_words