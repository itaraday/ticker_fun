from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from nltk import sent_tokenize, word_tokenize
import string
import re


lemmatizer = WordNetLemmatizer()
try:
    stop = stopwords.words('english')
except:
    import nltk
    nltk.download('stopwords')
    stop = stopwords.words('english')
newstop_words = ['yolo']
stop.extend(newstop_words)


def cleanup(text, remove_punct=False):
    clean = ' '.join([word.lower() for word in text.split() if word.lower() not in (stop)])
    if remove_punct:
        clean = clean.translate(str.maketrans('', '', string.punctuation))
    clean = [lemmatizer.lemmatize(word) for word in clean.split()]
    return clean

def cleanup_twitter(text):
    text = re.sub(r'\n+', ' ', text).strip()
    # remove twitter Return handles (RT @xxx:)
    text = re.sub(r'RT @[\w]*:', '', text)
    # remove twitter handles (@xxx)
    text = re.sub(r'@[\w]*', '', text)
    # remove URL links (httpxxx)
    text = re.sub(r"(?:\@|http?\://|https?\://|www)\S+", '', text)
    #sentences = sent_tokenize(text)
    return text

def make_sentences_reddit(text):
    #cleanup whitespace
    text = text.replace('&nbsp;', '\n')
    text = re.sub(r'\n+', ' ', text).strip()
    text = re.sub(r'\s\s+', ' ', text).strip()

    #cleanup quotes
    text = re.sub(r'\"?\\?&?gt;?', '', text)

    # text = re.sub(r'\.+', '.', text).strip()
    # text = text.replace('\n', '. ')

    #turn headingings into new pharagraph
    def checkheading(match, symbol):
        groups = list(match.groups())
        regex = r"\{}+".format(symbol)
        groups[0] = re.sub(regex, '. ', groups[0])
        groups[2] = re.sub(regex, '', groups[2])
        return ''.join(groups)

    text = re.sub(r'(\*\*+)(.*?)(\*\*+)', lambda x: checkheading(x, symbol='*'), text)#lambda x: ". {}".format(x.group().replace('*', '')), text)
    text = re.sub(r'(\#+)(.*?)(\#+)', lambda x: checkheading(x, symbol='#'), text)
    #text = re.sub(r'(\*\*+)(.*?)(\*\*+)', lambda x: ". {}".format(x.group().replace('*', '')), text)
    #text = re.sub(r'(\#+)(.*?)(\#+)', lambda x: "{}".format(x.group().replace('#', '')), text)
    #text = text.replace('\n.', '')
    sentences = sent_tokenize(text)
    return sentences

def remove_urls(text):
    # cleanup hyperlinkes. If it is formatted remove the hyperlink
    text = re.sub(r'\(https?:\/\/[^\s]*\s?', ' ', text, flags=re.MULTILINE)
    return text

def mark_common_stock(symbol):
    symbol = symbol.lower()
    common = False
    if symbol in stop:
        common = True
    return common
