from nltk.stem import PorterStemmer
import hashlib

ps = PorterStemmer()

class Token:
    def __init__(self, tok_str: str) -> None:
        self.tok_str: str = ps.stem(tok_str)
    
    def __eq__(self, value: object) -> bool:
        return self.tok_str.lower() == value.tok_str.lower()

    def __repr__(self) -> str:
        return self.tok_str
    
    def __hash__(self) -> int:
        return int(hashlib.sha256(self.tok_str.encode()).hexdigest(), 16)